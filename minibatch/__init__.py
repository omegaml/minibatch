from mongoengine import connect, register_connection
from mongoengine.connection import get_db

from minibatch.models import Stream, Buffer
from minibatch.window import RelaxedTimeWindow, FixedTimeWindow, CountWindow


def streaming(name, interval=None, size=None, emitter=None,
              relaxed=True, keep=False, **kwargs):
    """
    make and call a streaming function

    Usage:
        # fixed-size stream
        @stream(size=n)
        def myproc(window):
            # process window.data

        # time-based stream
        @stream(interval=seconds)
        def myproc(window):
            # process window.data

        # arbitrary WindowEmitter subclass
        @stream(emitter=MyWindowEmitter):
        def myproc(window):
            # process window.data

    If interval is given, a RelaxedTimeWindow into the stream is created. A
    RelaxedTimeWindow will call the decorated function with a window of data
    since the last time it did so. To get a FixedTimeWindow, specify
    relaxed=False.

    If size is given, a CountWindow into the stream is created. A CountWindow
    will call the decorated function with a window of exactly #size of
    objects in data.

    If a WindowEmitter subclass is given, an instance of that emitter is created
    and passed any optional kwargs and it's run() method is called. This emitter
    may process the buffered data in any arbitrary way it chooses.

    :param name: the stream name
    :param interval: interval in seconds
    :param size: interval in count of buffered, unprocessed objects in stream
    :param keep: optional, keep Buffer and Stream data. defaults to False
    :param relaxed: optional, defaults to True. chooses between Relaxed and
    FixedTimeWindow
    :param emitter: optional, a WindowEmitter subclass (advanced)
    """

    def inner(fn):
        fn._count = 0
        stream = Stream.get_or_create(name, interval=interval or size)
        if interval and emitter is None:
            if relaxed:
                em = RelaxedTimeWindow(name, emitfn=fn, interval=interval)
            else:
                em = FixedTimeWindow(name, emitfn=fn, interval=interval)
        elif size and emitter is None:
            em = CountWindow(name, emitfn=fn, interval=size)
        elif emitter is not None:
            em = emitter(name, emitfn=fn,
                         interval=interval or size,
                         **kwargs)
        else:
            raise ValueError("need either interval=, size= or emitter=")
        em.persist(keep)
        em.run()

    inner.apply = lambda fn: inner(fn)
    return inner


class IntegrityError(Exception):
    pass


def setup(url=None, dbname=None, alias=None, **kwargs):
    alias = alias or 'minibatch'
    connect(alias=alias, db=dbname, host=url, connect=False, **kwargs)
    return get_db(alias=alias)
