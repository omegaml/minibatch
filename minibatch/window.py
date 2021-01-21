from concurrent.futures import Future, ProcessPoolExecutor

import datetime
import logging
import time
from queue import Empty

from minibatch import Buffer, Stream, logger
from minibatch.marshaller import SerializableFunction, MinibatchFuture
from minibatch.models import Window


class WindowEmitter(object):
    """
    a window into a stream of buffered objects

    WindowEmitter.run() implements the generic emitter protocol as follows:

    1. determine if a window is ready to be processed
    2. retrieve the data from the buffer to create a Window
    3. process the data (i.e. mark the buffered data processed)
    4. run the emit function on the window
    5. commit (emit function successful) or undo (exception raised)

    Note that run() is blocking. Between running the protocol,
    it will sleep to conserve resources.

    Each time run() wakes up, it will call the following methods in turn:

        window_ready() - called to determine if the buffer contains enough
                         data for a window.
        query()        - return the Buffer objects to process
        process()      - optionally process the data. By default his just
                         marks the Buffer objects returned by query() as
                         processed=True
        emit()         - emit a Window, this calls the streaming function
                         which may optionally return a result that can be
                         forwarded to a sink, see forward()
        timestamp()    - timestamp the stream for the next processing
        commit()       - commit processed data back to the buffer. by
                         default this means removing the objects from the
                         buffer and deleting the window.
        forward()      - if a forward function has been defined, call it with
                         the result of emit()
        sleep()        - sleep until the next round

    Use timestamp() to mark the stream (or the buffer data) for the next
    round. Use sleep() to set the amount of time to sleep. Depending on
    the emitter's semantics this may be a e.g. a fixed interval or some
    function of the data.

    WindowEmitter implements several defaults:

        process() - mark all data returned by query() as processed
        sleep()   - sleep self.interval / 2 seconds
        undo()    - called if the emit function raises an exception. marks
                    the data returned by query() as not processed and deletes
                    the window

    For examples of how to implement a custom emitter see TimeWindow,
    CountWindow and SampleFunctionWindow.

    Notes:
        * there should only be one WindowEmitter per stream. This is a
          a limitation of the Buffer's way of marking documentes as processed
          (a boolean flag). This decision was made in favor of performance and
          simplicity.  Supporting concurrent emitters would mean each Buffer object
          needs to keep track of which emitter has processed its data and make
          sure Window objects are processed by exactly one emitter.

        * to stop the run() method, specify a threading or multiprocessing Queue
          and send True
    """

    def __init__(self, stream_name, interval=None, processfn=None,
                 emitfn=None, emit_empty=False, executor=None,
                 max_workers=None, stream=None, stream_url=None,
                 forwardfn=None, queue=None):
        self.stream_name = stream_name
        self.interval = interval if interval is not None else 1
        self.emit_empty = emit_empty
        self.emitfn = emitfn
        self.processfn = processfn
        self.executor = (executor or ProcessPoolExecutor(max_workers=max_workers))
        self._stream = stream
        self._stream_url = stream_url
        self._delete_on_commit = True
        self._forwardfn = forwardfn
        self._stop = False
        self._queue = queue

    def query(self, *args):
        raise NotImplementedError

    def window_ready(self):
        """ return a tuple of (ready, qargs) """
        raise NotImplementedError

    def timestamp(self, query_args):
        self.stream.modify(last_read=datetime.datetime.utcnow())

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

    def undo(self, qs, window=None):
        for obj in qs:
            obj.modify(processed=False)
        if window is not None and hasattr(window, 'delete'):
            window.delete()
        return qs

    def persist(self, flag=True):
        self._delete_on_commit = not flag

    def commit(self, qs, window):
        if not self._delete_on_commit:
            window.modify(processed=True)
            return
        for obj in qs:
            obj.delete()
        if window is not None and hasattr(window, 'delete'):
            window.delete()

    def emit(self, qs, query_args=None):
        window = Window(stream=self.stream.name,
                        query=query_args,
                        data=[obj.data for obj in qs]).save()
        if self.emitfn:
            logging.debug("calling emitfn")
            try:
                sjob = SerializableFunction(self.emitfn, window)
                future = self.executor.submit(sjob)
            except Exception:
                raise
        else:
            future = Future()
            future.set_result(window)
        future = MinibatchFuture(future, window=window)
        return future

    def forward(self, data):
        if self._forwardfn:
            self._forwardfn(data)

    def sleep(self):
        time.sleep((self.interval or self.stream.interval) / 2.0)

    def should_stop(self):
        if self._queue is not None:
            try:
                stop_message = self._queue.get(block=False)
                logger.debug("queue result {}".format(self._stop))
            except Empty:
                logger.debug("queue was empty")
            else:
                if stop_message:
                    self._stop = True
        logger.debug("should stop")
        return self._stop

    def run(self, blocking=True):
        while not self.should_stop():
            self._run_once()
            logger.debug("sleeping")
            self.sleep()
            logger.debug("awoke")
            if not blocking:
                break
        if blocking:
            # if we did not block, keep executor running
            try:
                self.executor.shutdown(wait=True)
            except Exception as e:
                logger.debug(e)
        logger.debug('stopped running')

    def _run_once(self):
        logger.debug("testing window ready")
        ready, query_args = self.window_ready()
        if ready:
            logger.debug("window ready")
            qs = self.query(*query_args)
            qs = self.process(qs)
            self.timestamp(*query_args)
            # note self.emit is usin an async executor
            # that returns a future
            if qs or self.emit_empty:
                logger.debug("Emitting")
                future = self.emit(qs, query_args)
                logger.debug("got future {}".format(future))
                future['qs'] = qs
                future['query_args'] = query_args

                def emit_done(future):
                    # this is called once upon future resolves
                    future = MinibatchFuture(future)
                    logger.debug("emit done {}".format(future))
                    qs = future.qs
                    window = future.window
                    try:
                        data = future.result() or window
                    except Exception:
                        self.undo(qs, window)
                    else:
                        self.commit(qs, window)
                        if isinstance(data, Window):
                            data = data.data
                        self.forward(data)
                    finally:
                        logger.debug('emit done')
                    self.sleep()

                future.add_done_callback(emit_done)


class FixedTimeWindow(WindowEmitter):
    """
    a fixed time-interval window

    Yields windows of all data retrieved in fixed intervals of n
    seconds. Note that windows are created in fixed-block sequences,
    i.e. in steps of n_seconds since the start of the stream. Empty
    windows are also emitted. This guarantees that any window
    contains only those documents received in that particular window.
    This is useful if you want to count e.g. the number of events
    per time-period.

    Usage:

        @stream(name, interval=n_seconds)
        def myproc(window):
            # ...
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.emit_empty = True

    def window_ready(self):
        last_read = self.stream.last_read
        now = datetime.datetime.utcnow()
        max_read = last_read + datetime.timedelta(seconds=self.interval)
        return now > max_read, (last_read, max_read)

    def query(self, *args):
        last_read, max_read = args
        fltkwargs = dict(stream=self.stream_name,
                         created__gte=last_read, created__lte=max_read)
        return Buffer.objects.no_cache().filter(**fltkwargs)

    def timestamp(self, *args):
        last_read, max_read = args
        self.stream.modify(last_read=max_read)
        self.stream.reload()

    def sleep(self):
        import time
        # sleep slightly longer to make sure the interval is complete
        # and all data had a chance to accumulate. if we don't do
        # this we might get empty windows on accident, resulting in
        # lost data
        now = datetime.datetime.utcnow()
        if self.stream.last_read > now - datetime.timedelta(seconds=self.interval):
            # only sleep if all previous windows were processed
            time.sleep(self.interval + 0.01)


class RelaxedTimeWindow(FixedTimeWindow):
    """
    a relaxed time-interval window

    Every interval n_seconds, yields windows of all data in the buffer
    since the last successful retrieval of data. This does _not_
    guarantee the data retrieved is in a specific time range. This is
    useful if you want to retrieve data every n_seconds but do not
    care when the data was inserted into the buffer.

    Usage:

        @stream(name, interval=n_seconds)
        def myproc(window):
            # ...
    """

    def query(self, *args):
        last_read, max_read = args
        fltkwargs = dict(stream=self.stream_name,
                         created__lte=max_read, processed=False)
        return Buffer.objects.no_cache().filter(**fltkwargs)


class CountWindow(WindowEmitter):
    def window_ready(self):
        fltkwargs = dict(stream=self.stream_name, processed=False)
        qs = Buffer.objects.no_cache().filter(**fltkwargs).limit(self.interval)
        n_docs = qs.count(with_limit_and_skip=True)
        self._qs = qs
        return n_docs >= self.interval, []

    def query(self, *args):
        return self._qs

    def timestamp(self, *args):
        self.stream.modify(last_read=datetime.datetime.utcnow())
        self.stream.reload()

    def sleep(self):
        import time
        time.sleep(0.1)
