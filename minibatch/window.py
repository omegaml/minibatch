import datetime

from minibatch import Buffer, Stream
from minibatch.models import Window


class WindowEmitter(object):
    """
    a window into a stream of buffered objects

    WindowEmitter.run() implements the generic emitter protocol as follows:

    1. determine if a window is ready to be processed
    2. retrieve the data from the buffer to create a Window
    3. process the data (i.e. mark the buffered data processed)
    4. run the emit function on the window

    Note that run() is blocking. Between running the protocol,
    it will sleep to conserve resources.

    Each time run() wakes up, it will call the following methods in turn:

        window_ready() - called to determine if the buffer contains enough
                         data for a window.
        query()        - return the Buffer objects to process
        process()      - process the data
        timestamp()    - timestamp the stream for the next processing
        commit()       - commit processed data back to the buffer. by
                         default this means removing the objects from the
                         buffer and deleting the window.
        sleep()        - sleep until the next round

    Use timestamp() to mark the stream (or the buffer data) for the next
    round. Use sleep() to set the amount of time to sleep. Depending on
    the emitter's semantics this may be a e.g. a fixed interval or some function
    of the data.

    WindowEmitter implements several defaults:

        process() - mark all data returned by query() as processed
        sleep()   - sleep self.interval / 2 seconds
        undo()    - called if the emit function raises an exception. marks
                    the data returned by query() as not processed and deletes
                    the window

    For examples of how to implement a custom emitter see TimeWindow, CountWindow
    and SampleFunctionWindow.

    Note there should only be one WindowEmitter per stream. This is a
    a limitation of the Buffer's way of marking documentes as processed (a boolean
    flag). This decision was made in favor of performance and simplicity.  Supporting
    concurrent emitters would mean each Buffer object needs to keep track of which
    emitter has processed its data and make sure Window objects are processed by
    exactly one emitter.
    """

    def __init__(self, stream, interval=None, processfn=None, emitfn=None,
                 emit_empty=False):
        self.stream_name = stream
        self.interval = interval
        self.emit_empty = emit_empty
        self.emitfn = emitfn
        self.processfn = processfn
        self._stream = None
        self._window = None  # current window if any
        self._delete_on_commit = True

    def query(self, *args):
        raise NotImplemented()

    def window_ready(self):
        """ return a tuple of (ready, qargs) """
        raise NotImplemented()

    def timestamp(self, query_args):
        self.stream.modify(query={}, last_read=datetime.datetime.now())

    @property
    def stream(self):
        if self._stream:
            return self._stream
        self._stream = Stream.get_or_create(self.stream_name)
        return self._stream

    def process(self, qs):
        if self.processfn:
            return self.processfn(qs)
        data = []
        for obj in qs:
            obj.modify(processed=True)
            data.append(obj)
        return data

    def undo(self, qs):
        for obj in qs:
            obj.modify(processed=False)
        if self._window:
            self._window.delete()
        return qs

    def persist(self, flag=True):
        self._delete_on_commit = not flag

    def commit(self, qs, window):
        if not self._delete_on_commit:
            window.modify(processed=True)
            return
        for obj in qs:
            obj.delete()
        window.delete()

    def emit(self, qs):
        self._window = Window(stream=self.stream.name,
                              data=[obj.data for obj in qs]).save()
        if self.emitfn:
            self._window = self.emitfn(self._window) or self._window
        return self._window

    def sleep(self):
        import time
        time.sleep((self.interval or self.stream.interval) / 2.0)

    def run(self):
        while True:
            ready, query_args = self.window_ready()
            if ready:
                qs = self.query(*query_args)
                qs = self.process(qs)
                if qs or self.emit_empty:
                    try:
                        window = self.emit(qs)
                    except Exception as e:
                        self.undo(qs)
                        print(str(e))
                    else:
                        self.commit(qs, window)
                    finally:
                        self.timestamp(*query_args)

            self.sleep()


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
        super(FixedTimeWindow, self).__init__(*args, **kwargs)
        self.emit_empty = True

    def window_ready(self):
        stream = self.stream
        last_read = stream.last_read
        now = datetime.datetime.now()
        max_read = last_read + datetime.timedelta(seconds=self.interval)
        return now > max_read, (last_read, max_read)

    def query(self, *args):
        last_read, max_read = args
        fltkwargs = dict(created__gte=last_read, created__lte=max_read)
        return Buffer.objects.no_cache().filter(**fltkwargs)

    def timestamp(self, *args):
        last_read, max_read = args
        self.stream.modify(query=dict(last_read__gte=last_read), last_read=max_read)
        self.stream.reload()

    def sleep(self):
        import time
        # we have strict time windows, only sleep if we are up to date
        if self.stream.last_read > datetime.datetime.now() - datetime.timedelta(seconds=self.interval):
            # sleep slightly longer to make sure the interval is complete
            # and all data had a chance to accumulate. if we don't do
            # this we might get empty windows on accident, resulting in
            # lost data
            time.sleep(self.interval + 0.25)


class RelaxedTimeWindow(WindowEmitter):
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

    def window_ready(self):
        stream = self.stream
        last_read = stream.last_read
        max_read = datetime.datetime.now()
        return True, (last_read, max_read)

    def query(self, *args):
        last_read, max_read = args
        fltkwargs = dict(created__gt=last_read, created__lte=max_read,
                         processed=False)
        return Buffer.objects.no_cache().filter(**fltkwargs)

    def timestamp(self, *args):
        last_read, max_read = args
        self.stream.modify(query=dict(last_read=last_read), last_read=max_read)
        self.stream.reload()


class CountWindow(WindowEmitter):
    def window_ready(self):
        qs = Buffer.objects.no_cache().filter(processed=False).limit(self.interval)
        self._data = list(qs)
        return len(self._data) >= self.interval, ()

    def query(self, *args):
        return self._data

    def timestamp(self, *args):
        self.stream.modify(query={}, last_read=datetime.datetime.now())

    def sleep(self):
        import time
        time.sleep(0.1)
