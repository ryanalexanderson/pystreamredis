# pystreamredis
A fast redis stream object for Python

This repo provides a Streams iterator object for access to interleaved
messages, sorted by index, one at a time. One creates a Streams object as follows:

.. code-block:: pycon

    >>> for this_message in r.streams(Stream0=0, Stream1=None, Stream2="$", Stream3=1530403200000):
    >>>      print(this_message)

This `r.streams` command returns a Streams object attached to the StrictRedis
instance `r`. The example above tracks the four Redis streams `Stream0`, `Stream1`, `Stream2`, and `Stream3`. Stream0 is
retrieved from epoch time_ms 0 (i.e. the entire message history), Stream1 and Stream2 begin listening for new
messages from the present time, and Stream3 from an arbitrary epoch time (July 1 2018, in this case).

An example of an individual `this_message` output is as follows:

.. code-block:: pycon

    >>> print(this_message)
    ('Stream0', b'1529416095111-4', {b'index': b'21'})

The tuple contains the following entries:

* **stream_name**: The name of the individual stream providing this message.
* **index**: The `<timestamp_ms>-<integer>` index of this message.
* **data**: The message data, as a dictionary.

The following optional keyword arguments control the flow and behaviour of the
iterator.

* **streams**: (default: None) Streams may be submitted as direct keyword arguments as in the example above. They may also be submitted as a separate dict, list, or set. This submission allows the stream names to be variables, any of the keywords listed below, or a stream name that does not conform to Python keyword rules. Values in a dict indicate the start time (None or "$" indicate present time, which is also the default for lists and sets).
* **count**: (default: 100) The number of messages per stream requested from the redis server at a single time. This value controls the internal tradeoff between the number of round-trip requests to the server made versus the memory size of the response that needs to be locally stored, but otherwise does not affect message flow.
* **block**: (default: 1000) As with the raw XREAD command, this value sets the time in milliseconds to block and wait for incoming messages. After waiting for `block` milliseconds, the behaviour of the iterator is controlled by `stop_on_timeout` and `timeout_response`, as described below. If set to `None`, there is no blocking. This value also controls the time before reconnect attempts in a ConnectionError failure where raise_connection_exceptions=False.
* **stop_on_timeout**: (default: False) If set to True, after the `block` period is over, the iterator will return `None` (or the `timeout_response` object if defined). If set to False, the `StopIteration` exception will be internally raised, naturally ending the iteration.
* **timeout_response**: (default: None) If `stop_on_timeout` is set to True, the object (of arbitrary type) defined by `timeout_response` will be returned in lieu of a message. If set to None, None will be returned.
* **raise_connection_exceptions**: (default: True) If True, then ConnectionError exceptions will be raised normally. If False, the exceptions will be returned in lieu of a message but not raised, making the iterator robust to temporary loss of connectivity. The caller retains the ability to use isinstance(this_message, ConnectionError) to detect these exceptions and raise them, or simply wait for connectivity to return.
