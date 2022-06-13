import logging
import os
from mongoengine import get_connection, ConnectionFailure
from pymongo.errors import AutoReconnect
from time import sleep

from minibatch._version import version  # noqa
from minibatch.models import Stream, Buffer, Window  # noqa

logger = logging.getLogger(__name__)
mongo_pid = None


def streaming(name, interval=None, size=None, emitter=None,
              relaxed=True, keep=False, url=None, sink=None,
              queue=None, source=None, blocking=True, **kwargs):
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
    return inner


def stream(name, url=None, ssl=False, **kwargs):
    cnx_kwargs = dict(url=url, ssl=ssl)
    kwargs.update(cnx_kwargs)
    return Stream.get_or_create(name, **kwargs)


class IntegrityError(Exception):
    pass


def make_emitter(name, emitfn, interval=None, size=None, relaxed=False,
                 url=None, sink=None, emitter=None, keep=False, queue=None,
                 source=None, cnx_kwargs=None, **kwargs):
    from minibatch.window import RelaxedTimeWindow, FixedTimeWindow, CountWindow

    if interval is None and size is None:
        size = 1

    forwardfn = sink.put if sink else None

    emitfn._count = 0
    cnx_kwargs = cnx_kwargs or {}
    cnx_kwargs.update(url=url) if url else None
    stream = Stream.get_or_create(name, interval=interval or size, **cnx_kwargs)
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
    if source:
        # starts a background thread that inserts source messages into the Buffer
        stream.attach(source, background=True)
    return em


def reset_mongoengine():
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

    def clean(d):
        if 'minibatch' in d:
            del d['minibatch']
        if 'default' in d:
            del d['default']

    clean(connection._connection_settings)
    clean(connection._connections)
    clean(connection._dbs)
    Window._collection = None
    Buffer._collection = None
    Stream._collection = None


def authenticated_url(mongo_url, authSource='admin'):
    if '+srv' in str(mongo_url):
        # all configuration is provided by the DNS
        # https://docs.mongodb.com/manual/reference/connection-string/#std-label-connections-dns-seedlist
        return mongo_url
    if 'authSource' not in str(mongo_url):
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
    reset_mongoengine()
    if False and fast_insert:
        # set writeConcern options
        # https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html?highlight=mongoclient
        # w = 0 - disable write ack
        # journal = False - do not wait for write
        kwargs['w'] = 0
        kwargs['journal'] = False
    kwargs.setdefault('uuidRepresentation', 'standard')
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
