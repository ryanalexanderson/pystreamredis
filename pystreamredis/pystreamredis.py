import logging
import sys
import functools
from concurrent.futures import ThreadPoolExecutor
import datetime
import time
from credis import Connection
from credis.geventpool import ResourcePool
from collections import deque
logging.basicConfig(
         format='%(asctime)s %(levelname)-8s %(message)s',
         level=logging.INFO)

def xrevrange(connection, name, start='+', finish='-', count=None):
    pieces = [start, finish]
    if count is not None:
        if not isinstance(count, int) or count < 1:
            raise RedisError("XREVRANGE count must be a positive integer")
        pieces.append("COUNT")
        pieces.append(str(count))

    return connection.execute('XREVRANGE', name, *pieces)

def xread(connection, streams, count=None, block=None):
    pieces = []
    if block is not None:
        if not isinstance(block, int) or block < 1:
            raise RedisError("XREAD block must be a positive integer")
        pieces.append("BLOCK")
        pieces.append(str(block))
    if count is not None:
        if not isinstance(count, int) or count < 1:
            raise RedisError("XREAD count must be a positive integer")
        pieces.append("COUNT")
        pieces.append(str(count))

    pieces.append("STREAMS")
    ids = []
    for partial_stream in streams.items():
        pieces.append(partial_stream[0])
        ids.append(partial_stream[1])

    pieces.extend(ids)
    x = connection.execute('XREAD', *pieces)
    return x


class RedisError(Exception):
    pass

class Streams(object):
    """
    Stream is an iterator object that provides record-by-record access to a
    collection of Redis Streams. Messages are returned in order of index, regardless of stream.
    Stream names and starting id's are provided through the 'stream' input dict and/or kwargs. After 'block' ms a
    timeout response will be returned (default 1000, or one second) but iteration will continue. If 'block' is set to
    None or False, then iteration will stop if no new data arrives. Iteration will also stop after a nonzero block time
    if 'stop_on_timeout' is explicitly set. Redis Exceptions will be raised during iteration if stop_on_exception is
    True (otherwise, the exception will be returned but not raised).
    """
    def __init__(self, redis_conn=None, streams=None, count=100, block=1000, timeout_response=None,
                 stop_on_timeout=False, raise_connection_exceptions=True, **kwargs):
        #cdef int block_int
        self.block_int = int(block) if block else 1

        #cdef float ts_last_xread
        self.ts_last_xread = 0

        #cdef float minimum_interval_between_xreads
        self.minimum_interval_between_xreads = 0.1

        #cdef float ts_start_xread
        self.ts_start_xread = 0

        #cdef bint future_is_done
        self.future_is_done = True

        self.BIG_NUMBER = b"99999999999999"
        self.topic_hit_limit = set()
        if isinstance(streams, str):
            streams=[streams]

        if streams is None:
            if not len(kwargs):
                raise RedisError("No streams specified, either in streams= or kwargs.")
            streams = {}
        elif isinstance(streams, set) or isinstance(streams, list):
            streams = dict([(x, "$") for x in streams])
        elif isinstance(streams, dict):
            pass
        else:
            raise RedisError("streams must be a string, dict, set, list, or None.")

        self.streams = {}
        streams.update(kwargs)
        for (k,v) in streams.items():
            if isinstance(k, bytes):
                self.streams[k.decode()] = v
            else:
                self.streams[k] = v

        if not isinstance(redis_conn,Connection) and not isinstance(redis_conn,ResourcePool):
            raise RedisError("Parameter 'redis_conn' must be a credis.Connection.")
        self.connection = redis_conn
        self.count = count
        self.timeout_response = timeout_response
        self.sanitize_stream_starts()
        self.buffer_dict = {k:deque() for k in self.streams.keys()}
        self.sample_message_sizes = dict()
        self.topic_hit_limit = set()
        self.remove_from_limit = list()
        self.update_last_and_limit()
        self.stop_on_timeout = stop_on_timeout if block else True
        self.raise_connection_exceptions = raise_connection_exceptions
        self.connectionError = False
        self.future_pool = ThreadPoolExecutor(1)
        self.future_streams = self.future_pool.submit(lambda x: dict(), None)
        self.future_streams_processed = False

    def process_future_update(self):
        result = self.future_streams.result()
        if not self.future_streams_processed:
          if result is not None:
              for incomingStreamBin, incomingList in result:
                incomingStream=incomingStreamBin.decode()
                logging.debug(f"Adding {len(incomingList)} entries to {incomingStream} (currently {len(self.buffer_dict[incomingStream])})")
                if len(incomingList):
                    self.buffer_dict[incomingStream]=self.buffer_dict.get(incomingStream,deque()) + deque(incomingList)
                    self.streams[incomingStream] = incomingList[-1][0]
                    if len(incomingList) == self.count:
                        self.topic_hit_limit.add(incomingStream)
                logging.debug(f"Now have {len(self.buffer_dict[incomingStream])} entries in {incomingStream})")

          self.future_streams_processed = True

        requestList = [thisStream for thisStream,thisList in self.buffer_dict.items() if len(thisList) <= self.count]
        if len(requestList):
            self.future_streams = self.future_pool.submit(self.get_streams, requestList)
            self.future_streams.add_done_callback(self.updateFutureStatus)
            logging.debug("submit stream")
        else:
            logging.debug("Not submitting stream")

    def updateFutureStatus(self, whatever):
        #print("Future is done")
        self.future_is_done = True

    def get_streams(self,requestList):
        self.future_is_done = False
        self.ts_start_xread = time.time()
        logging.debug("Done:False")
        r = xread(self.connection, {k: self.streams[k] for k in requestList}, self.count, self.block_int)
        logging.debug("Done:True")
        self.ts_last_xread = time.time()  # potential data race?
        #self.future_is_done = True
        self.future_streams_processed = False
        return r

    def fill_buffer_dict(self, block=None):
        if len(self.streams):
            print("fill xread")
            r = self.connection.xread(self.count, block, **self.streams)
            return r
        else: # You want to listen to nothing? Sure, why not.
            time.sleep(self.block_int / 1000.0)
            return None

    def update_last_and_limit(self): # might be deleteable
        if self.buffer_dict is not None:
            for stream_name, record_list in self.buffer_dict.items():
                if len(record_list):  # always yes?
                    self.streams[stream_name] = record_list[-1][0]
                if len(record_list) == self.count:
                    self.topic_hit_limit.add(stream_name)

    def sanitize_stream_starts(self):
        for stream_name, next_index in self.streams.items():
            bad_format = False
            if isinstance(next_index, int):
                self.streams[stream_name] = str(next_index) + "-0"
                continue
            if isinstance(next_index, bytes):
                next_index = next_index.decode()

            if next_index == "$" or next_index is None:
                lastmsg = xrevrange(self.connection, stream_name, count=1)
                if lastmsg:
                    self.streams[stream_name] = lastmsg[0][0]
                else:
                    self.streams[stream_name] = 0
            elif "-" in next_index:
                split_string = next_index.split('-')
                try:
                    int(split_string[0])
                    int(split_string[1])
                except ValueError:
                    bad_format = True
            else:
                try:
                    int(next_index)
                    self.streams[stream_name] = str(next_index)+"-0"
                except ValueError:
                    bad_format = True

            if bad_format:
                raise ValueError("Streams values, if specified, must be integers, None, '$', or a Redis Index.")


    def get_lowest(self):
        lowest_timestamp_str = self.BIG_NUMBER
        lowest_index = self.BIG_NUMBER
        #cdef int lowest_index = 9999
        lowest_stream = None
        for stream_name in self.topic_hit_limit:
            record_list = self.buffer_dict[stream_name]

        if self.remove_from_limit:
            for remove_stream in self.remove_from_limit:
                self.topic_hit_limit.remove(remove_stream)
            self.remove_from_limit = []

        for stream_name, record_list in self.buffer_dict.items():
            if len(record_list):
                if record_list[0][0][0:13] < lowest_timestamp_str:
                    lowest_timestamp_str = record_list[0][0][0:13]
                    lowest_index = int(record_list[0][0][14:])
                    lowest_stream = stream_name
                elif record_list[0][0][0:13] == lowest_timestamp_str:
                    if int(record_list[0][0][14:]) < lowest_index:
                        lowest_index = int(record_list[0][0][14:])
                        lowest_stream = stream_name
        return lowest_timestamp_str, lowest_index, lowest_stream

    def resolve_possible_connection_errors(self):
        if self.connectionError:
            print("Connection not working; credis restore not written")
            #self.connection = StrictRedis(connection_pool=self.connection_pool)
            #self.connectionError = False

    def __iter__(self):
        return self

    def __next__(self):
      hit_zero=False
      while True:
        if time.time() - self.ts_last_xread > self.minimum_interval_between_xreads:
           #print("Time to pull")
           if any([True for x in self.buffer_dict.values() if len(x) < self.count]):
              time.sleep(0.0000001) # Can't explain why this speeds things up dramatically; threading thing, I suppose
              if self.future_is_done:# future_streams.done():
              #if self.future_streams.done():
                 self.process_future_update()
        (lowest_timestamp_str, lowest_index, lowest_stream) = self.get_lowest()
        if lowest_timestamp_str < self.BIG_NUMBER:
            entry = self.buffer_dict[lowest_stream].popleft()
            return lowest_stream, entry[0], entry[1]
        elif not hit_zero:
            logging.debug("Buffer empty")
            hit_zero=True

        #print(lowest_timestamp_str, [(k,len(v)) for k,v in self.buffer_dict.items()])

    def __repr__(self):
        return "<redis.client.Streams monitoring %d streams on %s" % (len(self.streams), repr(self.connection))
