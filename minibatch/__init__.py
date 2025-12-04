import logging
import os
import types
from mongoengine import get_connection, ConnectionFailure
from pymongo.errors import AutoReconnect
from time import sleep

from minibatch._version import version  # noqa
from minibatch.models import Stream, Buffer, Window  # noqa

logger = logging.getLogger(__name__)
mongo_pid = None


def streaming(name, fn=None, interval=None, size=None, emitter=None,
              relaxed=True, keep=False, url=None, sink=None,
              queue=None, source=None, blocking=True, **kwargs):
    """
    make and call a streaming function

    Usage:
        # fixed-size stream
        @stream(name, size=n)
        def myproc(window):
            # process window.data

        # time-based stream
        @stream(name, interval=seconds)
        def myproc(window):
            # process window.data

        # arbitrary WindowEmitter subclass
        @stream(name, emitter=MyWindowEmitter):
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

    def make(fn):
        return make_emitter(name, fn, interval=interval, size=size,
                            emitter=emitter, relaxed=relaxed, keep=keep,
                            url=url, sink=sink, queue=queue, source=source,
                            **kwargs)

    def inner(fn):
        if not hasattr(inner, '_em'):
            inner._em = make(fn)

        inner._em.run(blocking=blocking)

    inner.apply = lambda fn: inner(fn)
    inner.make = lambda fn: make(fn)
    return inner if fn is None else inner.apply(fn)


def stream(name, fn=None, url=None, **kwargs):
    if callable(fn):
        return streaming(name, url=url, **kwargs)(fn)
    kwargs.update(url=url)
    return Stream.get_or_create(name, **kwargs)


class IntegrityError(Exception):
    pass


def make_emitter(name, emitfn, interval=None, size=None, relaxed=False,
                 url=None, sink=None, emitter=None, keep=False, queue=None,
                 source=None, cnx_kwargs=None, **kwargs):
    from minibatch.window import RelaxedTimeWindow, FixedTimeWindow, CountWindow

    size = 1 if size is None and interval is None else size
    forwardfn = sink.put if sink else None
    if isinstance(emitfn, types.BuiltinFunctionType):
        orig_emitfn = emitfn
        emitfn = lambda *args, **kwargs: orig_emitfn(*args, **kwargs)  # noqa
    emitfn._count = 0
    cnx_kwargs = cnx_kwargs or {}
    cnx_kwargs.update(url=url) if url else None
    stream = Stream.get_or_create(name, interval=interval or size, **cnx_kwargs)
    kwargs.update(stream=stream, emitfn=emitfn, forwardfn=forwardfn, queue=queue,
                  size=size, interval=interval)
    if emitter is not None:
        em = emitter(name, **kwargs)
    elif interval:
        if relaxed:
            em = RelaxedTimeWindow(name, **kwargs)
        else:
            em = FixedTimeWindow(name, **kwargs)
    elif size:
        em = CountWindow(name, **kwargs)
    else:
        raise ValueError("need either interval=, size= or emitter=")
    em.persist(keep)
    if source:
        # starts a background thread that inserts source messages into the Buffer
        stream.attach(source, background=True)
    return em


def patch_mongoengine():
    # this is to avoid mongoengine's MongoClient instances in subprocesses
    # resulting in "MongoClient opened before fork" warning
    # the source of the problem is that mongoengine stores MongoClients in
    # a module-global dictionary. here we simply clear that dictionary before
    # the connections are re-created in a forked process
    # note this doesn't actually disconnect it just deletes the MongoClient
    # see https://stackoverflow.com/a/49404748/890242
    #     https://github.com/MongoEngine/mongoengine/issues/1599#issuecomment-374901186
    from mongoengine import connection
    # There is a fix in mongoengine 18.0 that is supposed to introduce the same
    # behavior using disconnection_all(), however in some cases this is the
    # actual source of the warning due to calling connection.close()
    # see https://github.com/MongoEngine/mongoengine/pull/2038
    # -- the implemented solution simply ensures MongoClients get recreated
    #    whenever needed
    if not isinstance(connection._connection_settings, ProcessLocal):
        setattr(connection, '_connection_settings', ProcessLocal(connection._connection_settings))
        setattr(connection, '_connections', ProcessLocal(connection._connections))
        setattr(connection, '_dbs', ProcessLocal(connection._dbs))
    return


def authenticated_url(mongo_url, authSource='admin'):
    if '+srv' in str(mongo_url):
        # all configuration is provided by the DNS
        # https://docs.mongodb.com/manual/reference/connection-string/#std-label-connections-dns-seedlist
        return mongo_url
    if mongo_url and 'authSource' not in str(mongo_url):
        joiner = '&' if '?' in mongo_url else '?'
        mongo_url = '{}{}authSource={}'.format(mongo_url, joiner, authSource)
    return mongo_url


def connectdb(url=None, dbname=None, alias=None, authSource='admin',
              fast_insert=True, **kwargs):
    from mongoengine import connect
    from mongoengine.connection import get_db

    url = url or os.environ.get('MINIBATCH_MONGO_URL') or os.environ.get('MONGO_URL')
    url = authenticated_url(url, authSource=authSource) if authSource else url
    alias = alias or 'minibatch'
    patch_mongoengine()
    if False and fast_insert:
        # set writeConcern options
        # https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html?highlight=mongoclient
        # w = 0 - disable write ack
        # journal = False - do not wait for write
        kwargs['w'] = 0
        kwargs['journal'] = False
    kwargs.setdefault('uuidRepresentation', 'standard')
    kwargs.pop('ssl', None)  # since pymongo 4.0
    connect(alias=alias, db=dbname, host=url, connect=False, **kwargs)
    waitForConnection(get_connection(alias))
    return get_db(alias=alias)


def waitForConnection(client):
    _exc = None
    command = client.admin.command
    for i in range(100):
        try:
            # The ping command is cheap and does not require auth.
            command('ping')
        except (ConnectionFailure, AutoReconnect) as e:
            sleep(0.01)
            _exc = e
        else:
            _exc = None
            break
    if _exc is not None:
        raise _exc


class ProcessLocal(dict):
    def __init__(self, *args, cache=None, **kwargs):
        self._pid = os.getpid()
        self._cache = cache
        super().__init__(*args, **kwargs)

    def _check_pid(self):
        if self._pid != os.getpid():
            self.clear()
            self._pid = os.getpid()

    def __getitem__(self, k):
        self._check_pid()
        return (self._cache or super()).__getitem__(k)

    def __setitem__(self, k, v):
        self._check_pid()
        (self._cache or super()).__setitem__(k, v)

    def keys(self):
        self._check_pid()
        return (self._cache or super()).keys()

    def values(self):
        self._check_pid()
        return (self._cache or super()).values()

    def clear(self):
        self._cache.clear() if self._cache else None
        return super().clear()

    def __contains__(self, item):
        self._check_pid()
        return (self._cache or super()).__contains__(item)
