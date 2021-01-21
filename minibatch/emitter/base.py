from concurrent.futures import Future, ProcessPoolExecutor

import datetime
import logging

from minibatch import Stream, logger
from minibatch.marshaller import SerializableFunction, MinibatchFuture
from minibatch.models import Buffer


class Emitter(object):
    """
    the basic emitter

    Emitter.run() implements the generic emitter protocol as follows:

    1. determine if a window is ready to be processed
    2. retrieve the data from the buffer to create a Window
    3. process the data (i.e. mark the buffered data processed)
    4. run the emit function on the window
    5. commit (emit function successful) or undo (exception raised)

    Note that run() is blocking. Between running the protocol,
    it will sleep to conserve resources.

    Each time run() wakes up, it will call the following methods in turn:

        ready()         - called to determine if the buffer contains new data
        query()        - return the Buffer objects to process
        process()      - process the data
        emit()         - emit a Window
        timestamp()    - timestamp the stream for the next processing
        commit()       - commit processed data back to the buffer. by
                         default this means removing the objects from the
                         buffer and deleting the window.
        sleep()        - sleep until the next round

    Use timestamp() to mark the stream (or the buffer data) for the next
    round. Use sleep() to set the amount of time to sleep. Depending on
    the emitter's semantics this may be a e.g. a fixed interval or some
    function of the data.

    Emitter implements several defaults:

        process() - mark all data returned by query() as processed
        sleep()   - sleep self.interval / 2 seconds
        undo()    - called if the emit function raises an exception. marks
                    the data returned by query() as not processed and deletes
                    the window

    For examples of how to implement a custom emitter see TimeWindow,
    CountWindow and SampleFunctionWindow.

    Note there should only be one WindowEmitter per stream. This is a
    a limitation of the Buffer's way of marking documentes as processed
    (a boolean flag). This decision was made in favor of performance and
    simplicity.  Supporting concurrent emitters would mean each Buffer object
    needs to keep track of which emitter has processed its data and make
    sure Window objects are processed by exactly one emitter.
    """

    def __init__(self, stream_name, processfn=None,
                 emitfn=None, emit_empty=False, executor=None,
                 max_workers=None, stream=None, stream_url=None,
                 forwardfn=None):
        self.stream_name = stream_name
        self.emit_empty = emit_empty
        self.emitfn = emitfn
        self.processfn = processfn
        self.executor = (executor or ProcessPoolExecutor(max_workers=max_workers))
        self._stream = stream
        self._stream_url = stream_url
        self._delete_on_commit = True
        self._forwardfn = forwardfn
        self._stop = False

    def query(self, *args):
        now, last_read = args
        last_read, max_read = args
        fltkwargs = dict(stream=self.stream_name,
                         created__gte=last_read, created__lte=now)
        return Buffer.objects.no_cache().filter(**fltkwargs)

    def ready(self):
        """ return a tuple of (ready, qargs) """
        stream = self.stream
        last_read = stream.last_read
        now = datetime.datetime.utcnow()
        return now > last_read, (now, last_read)

    def timestamp(self, query_args):
        self.stream.modify(query={}, last_read=datetime.datetime.utcnow())

    @property
    def stream(self):
        if self._stream:
            return self._stream
        self._stream = Stream.get_or_create(self.stream_name,
                                            url=self._stream_url)
        return self._stream

    def process(self, qs):
        if self.processfn:
            return self.processfn(qs)
        data = []
        for obj in qs:
            obj.modify(processed=True)
            data.append(obj)
        return data

    def undo(self, qs, data):
        for obj in qs:
            obj.modify(processed=False)
        return qs

    def persist(self, flag=True):
        self._delete_on_commit = not flag

    def commit(self, qs, data):
        if self._delete_on_commit:
            for obj in qs:
                obj.delete()

    def emit(self, qs):
        data = list(doc for doc in qs)
        return self._run_emitfn(data)

    def _run_emitfn(self, data):
        if self.emitfn:
            logging.debug("calling emitfn")
            try:
                sjob = SerializableFunction(self.emitfn, data)
                future = self.executor.submit(sjob)
            except Exception:
                raise
        else:
            future = Future()
            future.set_result(data)
        future = MinibatchFuture(future, data=data)
        return future

    def forward(self, window):
        if self._forwardfn:
            self._forwardfn(window.data)

    def sleep(self):
        import time
        time.sleep(.01)

    def stop(self):
        self._stop = True

    def run(self):
        while not self._stop:
            logger.debug("testing window ready")
            ready, query_args = self.ready()
            if ready:
                logger.debug("data ready")
                qs = self.query(*query_args)
                qs = self.process(qs)
                # note self.emit is usin an async executor
                # that returns a future
                if qs or self.emit_empty:
                    logger.debug("Emitting")
                    future = self.emit(qs)
                    logger.debug("got future {}".format(future))
                    future['qs'] = qs
                    future['query_args'] = query_args

                    def emit_done(future):
                        # this is called once upon future resolves
                        future = MinibatchFuture(future)
                        logger.debug("emit done {}".format(future))
                        qs = future.qs
                        data = future.data
                        query_args = future.query_args
                        try:
                            data = future.result() or data
                        except Exception:
                            self.undo(qs, data)
                        else:
                            self.commit(qs, data)
                            self.forward(data)
                        finally:
                            self.timestamp(*query_args)
                        self.sleep()

                    future.add_done_callback(emit_done)
            logger.debug("sleeping")
            self.sleep()
            logger.debug("awoke")
        self.executor.shutdown(wait=True)
