Minibatch - Python Stream Processing for humans
===============================================

Pre-requisites:
    * a running MongoDB accessible to minibatch (docker run mongodb)

omega|ml provides a straight-forward, Python-native approach to mini-batch streaming and complex-event
processing that is highly scalable. Streaming primarily consists of

* a producer, which is some function inserting data into the stream
* a consumer, which is some function retrieving data from the stream

Instead of directly connection producers and consumers, a producer sends messages to a stream. Think
of a stream as an endless buffer, or a pipeline, that takes input from many producers on one end, and
outputs messages to a consumer on the other end. This transfer of messages happens asynchronously, that
is the producer can send messages to the stream independent of whether the consumer is ready to receive, and the
consumer can take messages from the stream independent of whether the producer is ready to send.

Unlike usual asynchronous messaging, however, we want the consumer to receive messages in small batches as
to optimize throughput. That is, we want the pipeline to *emit* messages only subject to some criteria
of grouping messages, where each group is called a *mini-batch*. The function that determines whether the
batching criteria is met (e.g. time elapsed, number of messages in the pipeline) is called *emitter strategy*,
and the output it produces is called *window*.

Thus in order to connect producers and consumers we need a few more parts to our streaming system:

* a :code:`Stream`, acting as the buffer where messages sent by producers are stored until the emitting
* a :code:`WindowEmitter` implementing the emitter strategy
* a :code:`Window` representing the output produced by the emitter strategy


.. note::

    The producer accepts input from some external system, say a Kafka queue. The producer's responsibility
    is to enter the data into the streaming buffer. The consumer uses some emitter strategy to produce
    a Window of data that is then forwarded to the user's processing code.

Creating a stream
-----------------

Streams can be created by either consumers or producers. A stream can be connected to by both.

.. code::

    from minibatch import Stream
    stream = Stream.get_or_create('test')

Implementing a Producer
-----------------------

.. code::

    # a very simple producer
    for i in range(100):
        stream.append({'date': datetime.datetime.now().isoformat()})
        sleep(.5)


Implementing a Consumer
-----------------------

.. code::

    # a fixed size consumer -- emits windows of fixed sizes
    from minibatch import streaming

    @streaming('test', size=2, keep=True)
    def myprocess(window):
        print(window.data)
    return window

    =>

    [{'date': '2018-04-30T20:18:22.918060'}, {'date': '2018-04-30T20:18:23.481320'}]
    [{'date': '2018-04-30T20:18:24.041337'}, {'date': '2018-04-30T20:18:24.593545'}
    ...


In this case the emitter strategy is :code:`CountWindow`. The following strategies are
available out of the box:

* :code:`CountWindow` - emit fixed-sized windows. Waits until at least *n* messages are
   available before emitting a new window
* :code:`FixedTimeWindow`- emit all messages retrieved within specific, time-fixed windows of
   a given interval of *n* seconds. This guarnatees that messages were received in the specific
   window.
* :code:`RelaxedTimeWindow` - every interval of *n* seconds emit all messages retrieved since
   the last window was created. This does not guarantee that messages were received in a given
   window.


Implementing a custom WindowEmitter
-----------------------------------

Custom emitter strategies are implemented as a subclass to :code:`WindowEmitter`. The main methods
to implement are

* :code:`window_ready` - returns the tuple :code:`(ready, data)`, where ready is True if there is data
     to emit
* :code:`query` - returns the data for the new window. This function retrieves the :code:`data` part
     of the return value of :code:`window_ready`

See the API reference for more details.

.. code::

    class SortedWindow(WindowEmitter):
        """
        sort all data by value and output only multiples of 2 in batches of interval size
        """
        def window_ready(self):
            qs = Buffer.objects.no_cache().filter(processed=False)
            data = []
            for obj in sorted(qs, key=lambda obj : obj.data['value']):
                if obj.data['value'] % 2 == 0:
                    data.append(obj)
                    if len(data) >= self.interval:
                        break
            self._data = data
            return len(self._data) == self.interval, ()

        def query(self, *args):
            return self._data

