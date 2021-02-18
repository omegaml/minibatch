minibatch - Python Stream Processing for humans
===============================================

|build badge|

.. |build badge| image:: https://github.com/omegaml/minibatch/workflows/Python%20package/badge.svg
.. _CONTRIBUTING.md: https://github.com/omegaml/minibatch/blog/master/CONTRIBUTING.md

Dependencies:
    * a running MongoDB accessible to minibatch
    * Python 3.x
    * see extras & optional dependencies below for specific requirements

minibatch provides a straight-forward, Python-native approach to mini-batch streaming and complex-event
processing that is easily scalable. Streaming primarily consists of

* a producer, which is some function inserting data into the stream
* a consumer, which is some function retrieving data from the stream
* transform and windowing functions to process the data in small batches and in parallel

minibatch is an integral part of `omega|ml <https://github.com/omegaml/omegaml>`_, however also works independently. omega|ml is the Python DataOps and MLOps
platform for humans.

Features
--------

* native Python producers and consumers
* includes three basic Window strategies: CountWindow, FixedTimeWindow, RelaxedTimeWindow
* extensible Window strategies by subclassing and overriding a few methods
* scalable, persistent streams - parallel inserts, parallel processing of windows

A few hightlights

* creating a stream and appending data is just 2 lines of code
* producer and consumer stream code runs anywhere
* no dependencies other than mongoengine, pymongo
* extensible sources and sinks (already available: Kafka, MQTT, MongoDB collections, omega|ml datasets)
* a fully functional streaming web app can be built in less than 15 lines of code (using Flask)

Why is it called minibatch? Because it focuses on getting things done by using existing
technology, and making it easy to use this techonlogy. It may be minimalistic in approach, but maximises results.

Quick start
-----------

1. Install and setup

   .. code:: python

      $ pip install minibatch
      $ docker run -d -p 27017:27017 mongo

   See extras & optional dependencies below to select specific packages according
   to your deployment needs, e.g. for MQTT, Kafka, omega|ml

2. Create a stream producer or attach to a source

   .. code:: python

        import minibatch as mb
        stream = mb.stream('test')
        for i in range(100):
            stream.append({'date': datetime.datetime.utcnow().isoformat()})
            sleep(.5)

   Currently there is support for Kafka and MQTT sources. However
   arbitrary other sources can be added.

   .. code:: python

      from minibatch.contrib.kafka import KafkaSource
      source = KafkaSource('topic', urls=['kafka:port'])
      stream.attach(source)


3. Consume the stream

   .. code:: python

        from minibatch import streaming
	    @streaming('test', size=2, keep=True)
	    def myprocess(window):
	        print(window.data)
	    return window

	    =>
	    [{'date': '2018-04-30T20:18:22.918060'}, {'date': '2018-04-30T20:18:23.481320'}]
	    [{'date': '2018-04-30T20:18:24.041337'}, {'date': '2018-04-30T20:18:24.593545'}
	    ...

   `myprocess` is called for every N-tuple of items (`size=2`)  appended to the stream by the producer(s).
   The frequency is determined by the emitter strategy. This can be configured or changed for a custom
   emitter strategy, as shown in the next step.

4. Configure the emitter strategy

   Note the `@streaming` decorator. It implements a blocking consumer that delivers batches
   of data according to some strategy implemented by a WindowEmitter. Currently `@streaming`
   provides the following interface:

    * `size=N` - uses the :code:`CountWindow` emitter
    * `interval=SECONDS` - uses the :code:`RelaxedTimeWindow` emitter
    * `interval=SECONDS, relaxed=False` - uses the :code:`FixedTimeWindow` emitter
    * `emitter=CLASS:WindowEmitter` - uses the given subclass of a :code:`WindowEmitter`
    * `workers=N` - set the number of workers to process the decorated function, defaults to number of CPUs
    * `executor=CLASS:Executor` - the asynchronous executor to use, defaults to :code:`concurrent.futures.ProcessPoolExecutor`


5. Write a flask app as a streaming source

   This is a simple helloworld-style streaming application that is fully
   functional and distributable.

   .. code:: python

       # app.py
       def consumer(url):
          @streaming('test-stream', url=url)
          def processing(window):
             ... # whatever processing you need to do

       if __name__ == '__main__':
           app = StreamingApp()
           app.start_streaming(consumer)
           app.run()

       # run the app (check status at http://localhost:5000/status)
       $ python app.py

       # in an other process, stream data
       $ python
       [] import minibatch as mb
          stream = mb.stream('test-stream')
          stream.append(dict(data='foobar')

       Note there is no UI in this example, however the data is processed as
       it comes in. To add a UI, specify using @app.route, as for any flask app,
       write the processed data into a sink that the UI can access. For a
       full example see help(minibatch.contrib.apps.omegaml.StreamingApp)



Stream sources
--------------

Currently provided in :code:`minibatch.contrib`:

* KafkaSource - attach a stream to a Apache Kafka topic
* MQTTSource - attach to an MQTT broker
* MongoSource - attach to a MongoDB collection
* DatasetSource - attach to a omega|ml dataset
* CeleryEventSource - attach to a Celery app event dispatcher

Stream sources are arbitrary objects that support the :code:`stream()`
method, as follows.

.. code:: python

    class SomeSource:
        ...
        def stream(self, stream):
            for data in source:
                stream.append(data)


Stream Sinks
------------

The result of a stream can be forwarded to a sink. Currently
provided sinks in :code:`minibatch.contrib` are:

* KafkaSink - forward messagess to a Apache Kafka topic
* MQTTSink  - forward messages to an MQTT broker
* MongoSink - forward messages to a MongoDB collection
* DatasetSink - write to a omega|ml dataset

Stream sinks are arbitrary objects that support the :code:`put()`
method, as follows.

.. code:: python

    class SomeSink:
        ...
        def put(self, message):
            sink.send(message)


Window emitters
---------------

minibatch provides the following window emitters out of the box:

* :code:`CountWindow` - emit fixed-sized windows. Waits until at least *n* messages are
   available before emitting a new window
* :code:`FixedTimeWindow`- emit all messages retrieved within specific, time-fixed windows of
   a given interval of *n* seconds. This guarantees that messages were received in the specific
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

.. code:: python

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


What is streaming and how does minibatch implement it?
------------------------------------------------------

*Concepts*

Instead of directly connection producers and consumers, a producer sends messages to a stream. Think
of a stream as an endless buffer, or a pipeline, that takes input from many producers on one end, and
outputs messages to a consumer on the other end. This transfer of messages happens asynchronously, that
is the producer can send messages to the stream independent of whether the consumer is ready to receive, and the  consumer can take messages from the stream independent of whether the producer is ready to send.

Unlike usual asynchronous messaging, however, we want the consumer to receive messages in small batches to optimize throughput. That is, we want the pipeline to *emit* messages only subject to some criteria
of grouping messages, where each group is called a *mini-batch*. The function that determines whether the
batching criteria is met (e.g. time elapsed, number of messages in the pipeline) is called *emitter strategy*,
and the output it produces is called *window*.

Thus in order to connect producers and consumers we need the following parts to our streaming system:

* a :code:`Stream`, keeping metadata for the stream such as its name and when it was created, last read etc.
* a :code:`Buffer` acting as the buffer where messages sent by producers are stored until the emitting
* a :code:`WindowEmitter` implementing the emitter strategy
* a :code:`Window` representing the output produced by the emitter strategy

.. note::

    The producer accepts input from some external system, say an MQTT end point. The producer's responsibility is to enter the data into the streaming buffer.
    The consumer uses an emitter strategy to produce a Window of data that is then forwarded to the user's processing code.

*Implementation*

minibatch uses MongoDB to implement Streams, Buffers and Windows. Specifically, the following collections are used:

* `stream` - represents instances of `Stream`, each document is a stream with a unique name
* `buffer` - a virtually endless buffer for all streams in the system, each document contains one message of a stream
* `window`- each document represents the data as emitted by the particular emitter strategy

By default messages go through the following states

1. upon append by a producer: message is inserted into `buffer`, with flag `processed = False`
2. upon being seen by an emitter: message is marked as `processed = True`
3. upon being emitted: message is copied to `window`, marked `processed = False` (in Window)
4. upon emit success (no exceptions raised by the emit function): message is deleted from `buffer`
   and marked `processed = True` in `window`

Notes:

* emitters typically act on a collection of messages, that is steps 2 - 4 are applied to more
  than one message at a time

* to avoid deleting messages from the buffer, pass `@streaming(..., keep=True)`

* custom emitters can modify the behavior of both creating windows and handling the buffer by
  overriding the `process()`, `emit()` and `commit()` methods for each of the above steps
  2/3/4, respectively.

Extras & optional dependencies
------------------------------

minibatch provides the following pip install extras, which come with some
additional dependencies. Extras are installed by running

.. code:: bash

    $ pip install minibatch[<extra>|all]

Available extras are:

* :code:`apps` - adds StreamingApp for easy development & deployment of producers & consumers
* :code:`kafka` - to work with Kafka as a source or a sink
* :code:`mqtt` - to work with an MQTT broker as a source or a sink
* :code:`mongodb` - to work with MongoDB as a source or a sink
* :code:`omegaml` - to work with omega|ml datasets as a source or a sink
* :code:`all` - all of the above
* :code:`dev` - all of the above plus a few development packages


Further development
-------------------

Here are a couple of ideas to extend minibatch. Contributions are welcome.

* more examples, following typical streaming examples like word count, filtering
* more emitter strategies, e.g. for sliding windows
* performance testing, benchmarking
* distributed processing of windows via distributed framework such as celery, ray, dask
* extend emitters by typical stream operations e.g. to support operations like count, filter, map, groupby, merge, join
* add other storage backends (e.g. Redis, or some Python-native in-memory db that provides network access and an easy to use ORM layer, like mongoengine does for MongoDB)

Contributing
------------

We welcome any contributions - examples, issues, bug reports, documentation, code. Please see `CONTRIBUTING.md`_
for details. 

License
-------

Apache 2.0 licensed with "No Sell, Consulting Yes" clause. 
See LICENSE and LICENSE-NOSELLCLAUSE files.



