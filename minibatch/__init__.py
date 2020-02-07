import os

import logging

from minibatch._version import version  # noqa
from minibatch.models import Stream, Buffer  # noqa

logger = logging.getLogger(__name__)
mongo_pid = None


def streaming(name, interval=None, size=None, emitter=None,
              relaxed=True, keep=False, url=None, sink=None,
              queue=None, **kwargs):
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

    If a WindowEmitter subclass is given, an instance of that emitter is
    created and passed any optional kwargs and it's run() method is called.
    This emitter may process the buffered data in any arbitrary way it chooses.

    Args:
        name: the stream name
        interval: interval in seconds
        size: interval in count of buffered, unprocessed objects in stream
        emitter: optional, a WindowEmitter subclass (advanced)
        relaxed: optional, defaults to True. chooses between Relaxed and
        keep: optional, keep Buffer and Stream data. defaults to False
        url: the mongo db url
        **kwargs: kwargs passed to emitter class
    """

    def inner(fn):
        em = make_emitter(name, fn, interval=interval, size=size,
                          emitter=emitter, relaxed=relaxed, keep=keep,
                          url=url, sink=sink, queue=queue, **kwargs)
        em.persist(keep)
        em.run()

    inner.apply = lambda fn: inner(fn)
    return inner


def stream(name, url=None):
    return Stream.get_or_create(name, url=url)


class IntegrityError(Exception):
    pass


def make_emitter(name, emitfn, interval=None, size=None, relaxed=False,
                 url=None, sink=None, emitter=None, keep=False, queue=None, **kwargs):
    from minibatch.window import RelaxedTimeWindow, FixedTimeWindow, CountWindow

    if interval is None and size is None:
        size = 1

    forwardfn = sink.put if sink else None

    emitfn._count = 0
    stream = Stream.get_or_create(name, interval=interval or size, url=url)
    kwargs.update(stream=stream, emitfn=emitfn, forwardfn=forwardfn, queue=queue)
    if interval and emitter is None:
        if relaxed:
            em = RelaxedTimeWindow(name, interval=interval, **kwargs)
        else:
            em = FixedTimeWindow(name, interval=interval, **kwargs)
    elif size and emitter is None:
        em = CountWindow(name, interval=size, **kwargs)
    elif emitter is not None:
        em = emitter(name, emitfn=emitfn,
                     interval=interval or size,
                     **kwargs)
    else:
        raise ValueError("need either interval=, size= or emitter=")
    em.persist(keep)
    return em


def ensure_mongoclient_processlocal():
    # this is to avoid mongoengine's MongoClient instances in subprocesses
    # resulting in "MongoClient opened before fork" warning
    # the source of the problem is that mongoengine stores MongoClients in
    # a module-global dictionary. here we simply clear that dictionary before
    # the connections are re-created in a forked process
    global mongo_pid
    if mongo_pid is None:
        # remember the main process that created the first MongoClient
        mongo_pid = os.getpid()
    elif mongo_pid != os.getpid():
        # we're in a new process, disconnect
        # note this doesn't actually disconnect it just deletes the MongoClient
        from mongoengine import disconnect_all
        disconnect_all()


def connectdb(url=None, dbname=None, alias=None, **kwargs):
    from mongoengine import connect
    from mongoengine.connection import get_db, _connection_settings

    url = url or os.environ.get('MONGO_URL')
    alias = alias or 'minibatch'
    ensure_mongoclient_processlocal()
    connect(alias=alias, db=dbname, host=url, connect=False, **kwargs)
    if 'default' not in _connection_settings:
        # workaround to https://github.com/MongoEngine/mongoengine/issues/2239
        connect(alias='default', db=dbname, host=url, connect=False, **kwargs)
    return get_db(alias=alias)
