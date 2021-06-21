import logging
import uuid
import warnings
from concurrent.futures import ThreadPoolExecutor
import time
from collections import deque, Container, Mapping

from sys import getsizeof

from redis import DataError
from redis.exceptions import ConnectionError

logging.basicConfig(
         format='%(asctime)s %(levelname)-8s %(message)s',
         level=logging.INFO)

log = logging.getLogger(__name__)


def xrevrange(connection, name, start='+', finish='-', count=None, is_credis=False):
    pieces = [start, finish]
    if count is not None:
        if not isinstance(count, int) or count < 1:
            raise RedisError("XREVRANGE count must be a positive integer")
        pieces.append("COUNT")
        pieces.append(str(count))

    return connection.execute('XREVRANGE', name, *pieces) if is_credis else connection.execute_command('XREVRANGE', name, *pieces)

def xread(connection, streams, count=None, block=None, is_credis=False):
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
    try:
      resp = connection.execute('XREAD', *pieces) if is_credis else connection.execute_command('XREAD', *pieces)
    except ConnectionError as e:
      print(e)
      return None
    resp = connection.execute('XREAD', *pieces) if is_credis else connection.execute_command('XREAD', *pieces)
    return resp

def xreadgroup(connection, groupname, consumername, streams, count=None,
                   block=None, no_ack=False, is_credis=False):
        pieces = ['GROUP', groupname, consumername]
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError("XREADGROUP count must be a positive integer")
            pieces.append("COUNT")
            pieces.append(str(count))
        if block is not None:
            if not isinstance(block, int) or block < 0:
                raise DataError("XREADGROUP block must be a non-negative "
                                "integer")
            pieces.append("BLOCK")
            pieces.append(str(block))
        if no_ack:
            pieces.append("NOACK")
        if not isinstance(streams, dict) or len(streams) == 0:
            raise DataError('XREADGROUP streams must be a non empty dict')
        pieces.append('STREAMS')
        pieces.extend(streams.keys())
        pieces.extend(streams.values())

        if is_credis:
            resp = connection.execute('XREADGROUP', *pieces)
        else:
            resp = connection.execute_command('XREADGROUP', *pieces)
        return resp

def createGroup(connection, streams, group, is_credis=False):
    pieces = ["whoknows"]
    if is_credis:
        resp = connection.execute('XREADGROUP', *pieces)
    else:
        resp = connection.execute_command('XREADGROUP', *pieces)
    return resp


def checkRedisBase(redis_conn):
    execute_command = getattr(redis_conn, "execute", None)
    if callable(execute_command):
        return True
    execute_command = getattr(redis_conn, "execute_command", None)
    if callable(execute_command):
        return False
    raise ValueError("redis_conn must be a credis.Connection or a Redis.StrictRedis object.")


def processStreams(streams, kwargs, is_group=False, catchup=False):
    if isinstance(streams, str):
        streams = [streams]

    if streams is None:
        if not len(kwargs):
            raise RedisError("No streams specified, either in streams= or kwargs.")
        streams = {}
    elif isinstance(streams, set) or isinstance(streams, list):
        if is_group:
            if catchup:
                streams = dict([(x, 0) for x in streams])
            else:
                streams = dict([(x, ">") for x in streams])
        else:
            if catchup:
                raise NotImplementedError("Can't catchup on a non-group Stream yet. Working on it.")
            else:
                streams = dict([(x, "$") for x in streams])
    elif isinstance(streams, dict):
        pass
    else:
        raise RedisError("streams must be a string, dict, set, list, or None.")
    streamsout = {}
    wildcards = {}
    streams.update(kwargs)

    for (k, v) in streams.items():

        if isinstance(k, bytes):
            streamsout[k.decode()] = ">" if is_group and v=="$" else v
        else:
            if 1 in [c in k for c in "*[]?"]:  # to do: Escape characters
                wildcards[k] = ">" if is_group and v=="$" else v
            else:
                streamsout[k] = ">" if is_group and v=="$" else v
    return streamsout, wildcards


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
    def __init__(self, redis_conn=None, streams=None, count=5000, block=1000, timeout_response=None,
                 group=None, consumer_name=None, no_ack=True, catchup=True,
                 stop_on_timeout=False, raise_connection_exceptions=True,
                 min_xread_interval=0.1, min_check_stream_interval=0, **kwargs):
        #cdef int block_int
        self.block_int = int(block) if block else 1

        #cdef float ts_last_xread
        self.ts_last_xread = 0

        #cdef float minimum_interval_between_xreads
        self.minimum_interval_between_xreads = min_xread_interval

        #cdef float minimum_interval_between_streamchecks
        self.minimum_interval_between_streamchecks = min_check_stream_interval

        #cdef float ts_start_xread
        self.ts_start_xread = 0

        #cdef bint future_is_done
        self.future_is_done = True

        self.BIG_NUMBER = b"99999999999999"
        self.topic_hit_limit = set()
        self.group = group
        self.streams, self.wildcard_streams = processStreams(streams, kwargs=kwargs,
                                                             is_group= group is not None,
                                                             catchup=catchup)
        self.is_credis = checkRedisBase(redis_conn)
        self.connection = redis_conn
        self.timeout_response = timeout_response
        self.sanitize_stream_starts()
        self.sample_message_sizes = dict()
        self.topic_hit_limit = set()
        self.remove_from_limit = list()
        self.stop_on_timeout = stop_on_timeout if block else True
        self.raise_connection_exceptions = raise_connection_exceptions
        self.connectionError = False
        self.future_pool = ThreadPoolExecutor(1)
        self.future_streams = self.future_pool.submit(lambda x: dict(), None)
        self.future_streams_processed = False
        self.buffer_dict = {k:deque() for k in self.streams.keys()}
        self.empty_pull = False
        self.desired_response_size = count #!!!
        self.latest_response_size = 0
        self.count = count # Placeholder to start
        self.hit_zero_time = 0
        self.refresh_streams()
        self.update_last_and_limit()
        self.counter = 0
        self.perform_curve=deque(maxlen=20)
        self.ts_end_xread = 0
        self.no_ack = no_ack
        self.nack_flag = False
        self.group_cactchup = True
        self.current_id = None
        self.consumer_name = consumer_name
        if self.group is not None:
          log.warn("group autocreation not sorted out yet.")
          #createGroup(self.connection, self.streams, self.group)
          if consumer_name is None:
              self.consumer_name = uuid.uuid4()
              if not self.no_ack:
                  warnings.warn("Autogenerated consumer id should not be used with no_ack=False.")
                  log.error("consumer name autocreation not sorted out yet.")
                  raise Exception("consumer name autocreation not sorted out yet.")

    def create_groups(self, start="$", err_on_exist=False):
        if isinstance(start,int): start = str(start)

        if isinstance(start,str): start = [(k,start) for k in self.streams.keys()]
        elif isinstance(start,dict): start = [(k,start[k]) for k in self.streams.keys()]
        else:
            raise DataError("If specified, 'start' must be an int, string, or dict.")
        for stream, startval in start:
            pass
            #XGROUP here


    def refresh_streams(self, from_start=False):
        # from_start will override the default for the start time associated with the wildcard.
        # For refreshes done *after* the iterator has begun, odds are that you will not want to leave
        # out the first couple stream records that occurred before the new stream was detected.
        keys_to_add = {}
        for thisWildCard, thisWildCardValue in self.wildcard_streams.items():
            for key in self.connection.scan_iter(match=thisWildCard): # hope this isn't too big
                if key not in self.streams:
                    keys_to_add[key] = 0 if from_start else thisWildCardValue
        pipe = self.connection.pipeline()
        for k in keys_to_add.keys():
            pipe.type(k)

        new_streams = dict([(k,v) for k,v in zip(keys_to_add.keys(), pipe.execute()) if v == b'stream'])
        if len(new_streams):
          log.info("Adding streams: {new_streams}".format(new_streams=new_streams))
          self.streams.update(new_streams)
        return new_streams

    def process_future_update(self):
        result = self.future_streams.result()
        if not self.future_streams_processed:
          if result is not None:
              for incomingStreamBin, incomingList in result:
                incomingStream=incomingStreamBin.decode() if not isinstance(incomingStreamBin,str) else incomingStreamBin 
                log.debug("Adding {len(incomingList)} entries to {incomingStream} (currently {len(self.buffer_dict[incomingStream])})")
                if len(incomingList):
                    self.buffer_dict[incomingStream]=self.buffer_dict.get(incomingStream,deque()) + deque(incomingList)
                    self.streams[incomingStream] = incomingList[-1][0] if self.group is None else ">"
                    if len(incomingList) == self.count:
                        self.topic_hit_limit.add(incomingStream)
                log.debug("Now have {0} entries in {1})".format(len(self.buffer_dict[incomingStream]),incomingStream))

          self.future_streams_processed = True

        requestList = [thisStream for thisStream,thisList in self.buffer_dict.items() if len(thisList) <= self.count]
        if len(requestList):
            self.future_streams = self.future_pool.submit(self.get_streams, requestList)
            self.future_streams.add_done_callback(self.updateFutureStatus)
            log.debug("submit stream")
        else:
            log.debug("Not submitting stream")

    def updateFutureStatus(self, whatever):
        self.future_is_done = True

    def get_streams(self,requestList):
        self.future_is_done = False
        self.ts_start_xread = time.time()
        log.debug("Done:False, count = " + str(self.count))
        if self.group is None:
            r = xread(self.connection, {k: self.streams[k] for k in requestList},
                      self.count, self.block_int, self.is_credis)
        else:
            r = xreadgroup(self.connection, self.group, self.consumer_name,
                           {k: self.streams[k] for k in requestList},
                           count=self.count, block=self.block_int, no_ack=self.no_ack, is_credis=self.is_credis)

        # Calculate and explore count stats
        new_end_xread = time.time() + 1e-8
        rate = self.counter/(new_end_xread-self.ts_end_xread)
        self.perform_curve.append((self.count,rate))
        log.debug("Count, Rate: {0},{1}".format(self.count, rate))
        if len(self.perform_curve)==1:
            adjust = 0.25
        elif len(self.perform_curve)<5:
            pass
        self.counter=0
        self.ts_end_xread = new_end_xread

#        log.debug(f"read time: {time.time()-self.ts_start_xread}, time since last empty: {time.time()-self.hit_zero_time}")
#        log.debug(f"Changing count from {self.count} to {newcount}")

        self.empty_pull = r is None or not len(r)
        log.debug("Done:True")
        self.ts_last_xread = time.time()  # potential data race?
        self.future_streams_processed = False
        return r

    def update_last_and_limit(self): # might be deleteable
        if self.buffer_dict is not None:
            for stream_name, record_list in self.buffer_dict.items():
                if len(record_list):  # always yes?
                    self.streams[stream_name] = record_list[-1][0] if self.group is None else ">"
                if len(record_list) == self.count:
                    self.topic_hit_limit.add(stream_name)

    def sanitize_stream_starts(self):
        for stream_name, next_index in self.streams.items():
            bad_format = False
            if self.group is not None:
                continue
            if isinstance(next_index, int):
                self.streams[stream_name] = str(next_index) + "-0"
                continue
            if isinstance(next_index, bytes):
                next_index = next_index.decode()

            if next_index == "$" or next_index is None:
                lastmsg = xrevrange(self.connection, stream_name, count=1, is_credis=self.is_credis)
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

        # Perhaps bypass this bs if it's just one stream
        for stream_name, record_list in self.buffer_dict.items():
            if len(record_list):
                candidate_lowest_str, candidate_index_str = record_list[0][0].split(b'-')
                if candidate_lowest_str < lowest_timestamp_str:
                    lowest_timestamp_str = candidate_lowest_str
                    lowest_index = int(candidate_index_str)
                    lowest_stream = stream_name
                elif candidate_lowest_str == lowest_timestamp_str:
                    if int(candidate_index_str) < lowest_index:
                        lowest_index = int(candidate_index_str)
                        lowest_stream = stream_name
        return lowest_timestamp_str, lowest_index, lowest_stream

    def nack(self):
        if self.current_id is None:
            warnings.warn("No current item to nack.")
            return
        if self.no_ack:
            warnings.warn("no_ack is set; nack command ignored.")
            return
        self.nack_flag = True


    def resolve_possible_connection_errors(self):
        if self.connectionError:
            print("Connection not working; credis restore not written")

    def __iter__(self):
        return self

    def __next__(self):
      if not self.no_ack and self.group is not None:
        if not self.nack_flag:
            resp = self.connection.execute("XACK", self.current_id[0], self.current_id[1])
        else:
            self.nack_flag=False

      hit_zero = False
      while True:
        if time.time() - self.ts_last_xread > self.minimum_interval_between_xreads:
           #print("Time to pull")
           if any([True for x in self.buffer_dict.values() if len(x) < self.count]):
              if self.minimum_interval_between_streamchecks and time.time() > self.minimum_interval_between_streamchecks:
                  self.refresh_streams(from_start=True)
              time.sleep(0.0000001) # Yield the thread
              if self.future_is_done:# future_streams.done():
                 self.process_future_update()
        (lowest_timestamp_str, lowest_index, lowest_stream) = self.get_lowest()
        if lowest_timestamp_str < self.BIG_NUMBER:
            entry = self.buffer_dict[lowest_stream].popleft()
            self.counter = self.counter + 1
            self.current_id = (lowest_stream,entry[0])
            return lowest_stream, entry[0], entry[1]

        self.current_id = None
        if not hit_zero:
            log.debug("Buffer empty")
            hit_zero=True
            self.hit_zero_time = time.time()

        else:
            if self.empty_pull: #1000*(time.time() - hit_zero_time) > self.block_int: # Timeout
                self.empty_pull = False
                if self.stop_on_timeout:
                    raise StopIteration
                else:
                    return self.timeout_response


        #print(lowest_timestamp_str, [(k,len(v)) for k,v in self.buffer_dict.items()])

    def __repr__(self):
        return "<redis.client.Streams monitoring %d streams on %s" % (len(self.streams), repr(self.connection))
