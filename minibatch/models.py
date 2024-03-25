from logging import warning

import atexit
import datetime
import logging
import os
import random
import socket
import threading
from itertools import groupby
from mongoengine import Document
from mongoengine.errors import NotUniqueError, DoesNotExist
from mongoengine.fields import (StringField, IntField, DateTimeField,
                                ListField, DictField, BooleanField)
from pymongo.errors import DuplicateKeyError
from random import randint
from threading import Thread
from uuid import uuid4

from minibatch.util import ProcessLocal, resilient

STATUS_INIT = 'initialize'
STATUS_OPEN = 'open'
STATUS_CLOSED = 'closed'
STATUS_PROCESSED = 'processed'
STATUS_FAILED = 'failed'
STATUS_CHOICES = (STATUS_OPEN, STATUS_CLOSED, STATUS_FAILED)

logger = resilient(logging.getLogger(__name__))

# we don't propagate this logger to avoid logging housekeeping messages unless requested
hk_logger = logging.getLogger(__name__ + '.housekeeping')
hk_logger.propagate = False


class ThreadAlive(threading.Event):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.thread = None

    def __bool__(self):
        # return True if thread is alive (i.e. event is not set)
        return not self.is_set()

    def stop(self, wait=False, timeout=None):
        self.set()
        if wait:
            self.join(timeout=timeout)

    def join(self, timeout=5):
        if self.thread:
            self.thread.join(timeout)


class Batcher:
    """ A batching list-like

    This will batch up to batchsize items in an internal array of objects. To see if
    is full, check batcher.is_full. Note the internal array is only constrained by memory.

    To increase performance, Batcher will allocate an empty array of batchsize elements
    at creation time, or when setting .batchsize. If the array becomes too small, it will
    be increased by 10%. Note it will not be shrunk unless you call .clear(reset=True)

    Usage:
        batch = Batcher(batchsize=N)

        while True:
            batch.add(doc)
            if batch.is_full:
                process items
                batch.clear()
    """

    def __init__(self, batchsize=1):
        self._batch = []
        self.head = 0
        self.batchsize = batchsize

    @property
    def is_full(self):
        return self.head > self.batchsize + 1

    def add(self, doc):
        # protect against buffer overflow
        # update batch
        self._batch[self.head] = dict(doc)
        self.head += 1
        if len(self._batch) <= self.head:
            self._batch.extend([None] * int(self.batchsize * .1))

    def clear(self, reset=False):
        # reset batch
        self.head = 0
        if reset:
            self.batchsize = self.batchsize

    @property
    def batchsize(self):
        return self._batchsize

    @batchsize.setter
    def batchsize(self, v):
        # pre-allocate batch array
        self._batch = [None] * (v + 10)  # avoid eager extension on first fill
        self._batchsize = v
        self.clear()

    @property
    def batch(self):
        return self._batch[0:self.head]


class ImmediateWriter:
    @classmethod
    def write(cls, doc, batcher=None):
        """
        this does a fast, unchecked insert_one(), or insert_many() for batched

        No validation is done whatsoever. Only use this if you know what you are doing. This is
        250x times faster than Document.save() at the cost of not validating the documents.

        Args:
            doc (dict): the actual mongodb document to be written

        Returns:

        """
        cls: (ImmediateWriter, Document)
        if batcher is None:
            cls._get_collection().insert_one(doc)
        else:
            batcher.add(doc)
            if batcher.is_full:
                cls.flush(batcher)

    @classmethod
    def flush(cls, batcher=None):
        cls: (ImmediateWriter, Document)
        if batcher is not None:
            # the iterable is to avoid duplicate objects (batch op errors)
            cls._get_collection().insert_many(dict(d) for d in batcher.batch)
            batcher.clear()


class Window(ImmediateWriter, Document):
    """
    A Window is the data collected from a stream according
    to the WindowEmitter strategy.
    """
    stream = StringField(required=True)
    chord = StringField(required=True, default='default')
    created = DateTimeField(default=datetime.datetime.utcnow)
    data = ListField(default=[])
    processed = BooleanField(default=False)
    query = ListField(default=[])
    meta = {
        'db_alias': 'minibatch',
        'strict': False,  # support previous releases
        'indexes': [
            'created',
            'stream',
        ]
    }

    def __unicode__(self):
        return u"Window [%s] %s" % (self.created, self.data)


class Buffer(ImmediateWriter, Document):
    stream = StringField(required=True)
    chord = StringField(required=True, default='default')
    created = DateTimeField(default=datetime.datetime.utcnow)
    data = DictField(required=True)
    processed = BooleanField(default=False)
    meta = {
        'db_alias': 'minibatch',
        'strict': False,  # support previous releases
        'indexes': [
            'created',
            'stream',
        ]
    }

    def __unicode__(self):
        return u"Buffer created=[%s] chord=[%s] processed=%s data=%s" % (
            self.created, self.chord, self.processed, self.data)


class Stream(Document):
    """
    Stream provides metadata for a streaming buffer

    Streams are synchronized among multiple Stream clients using last_read.

    .. versionchanged:: 0.5.2
       Enable stream housekeeping by specifying max_age in seconds

    .. versionchanged:: 0.6.0
       Add support for multiple producers and consumers, using chords. See the Participant class.
    """
    name = StringField(default=lambda: uuid4().hex, required=True)
    status = StringField(choices=STATUS_CHOICES, default=STATUS_INIT)
    created = DateTimeField(default=datetime.datetime.utcnow)
    closed = DateTimeField(default=None)
    # interval in seconds or count in #documents
    interval = IntField(default=10)
    last_read = DateTimeField(default=datetime.datetime.utcnow)
    meta = {
        'db_alias': 'minibatch',
        'strict': False,  # support previous releases
        'indexes': [
            'created',  # most recent is last, i.e. [-1]
            {'fields': ['name'],
             'unique': True
             }
        ]
    }

    def __init__(self, *args, batchsize=1, max_age=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.ensure_initialized()
        self._batcher = None
        self._url = None
        self._cnx_kwargs = kwargs
        self._stream_source = None
        self.batchsize = batchsize
        self._max_age = max_age
        self._producer = None
        self._consumer = None
        self.autoclear(max_age)

    def ensure_initialized(self):
        if self.status == STATUS_INIT:
            self.modify({'status': STATUS_INIT},
                        status=STATUS_OPEN)

    @property
    def batchsize(self):
        return self._batcher.batchsize if self._batcher else 1

    @batchsize.setter
    def batchsize(self, size):
        if size > 1:
            self._batcher = self._batcher or Batcher(batchsize=size)
            self._batcher.batchsize = size
        else:
            self._batcher = None

    @property
    def as_producer(self):
        self._producer = self._producer or Participant.myself(self, 'producer')
        return self._producer

    @property
    def as_consumer(self):
        """ return self as a consumer

        .. versionadded:: 0.6.0
        """
        self._consumer = self._consumer or Participant.myself(self, 'consumer')
        return self._consumer

    @property
    def participants(self):
        """ return all participants for this stream

        .. versionadded:: 0.6.0
        """
        return Participant.for_stream(self)

    @property
    def producers(self):
        """ return all producers for this stream

        .. versionadded:: 0.6.0
        """
        return Participant.producers(self)

    @property
    def consumers(self):
        """ return all consumers for this stream

        .. versionadded:: 0.6.0
        """
        return Participant.consumers(self)

    def append(self, data, chord=None):
        t = datetime.datetime.utcnow()
        chord = chord or self.as_producer.select_chord()
        Buffer.write(dict(stream=self.name, chord=chord,
                          data=data or {}, processed=False, created=t), batcher=self._batcher)

    def flush(self):
        Buffer.flush(self._batcher)

    def clear(self):
        Buffer.objects.no_cache().filter(**{'stream': self.name}).delete()

    def attach(self, source, background=True):
        """
        use an external producer to start streaming
        """
        self._stream_source = source
        if not background:
            return source.stream(self)
        self._start_source(source)
        self.as_producer.start()

    def stop(self):
        """ stop the stream

        Stops the source and housekeeping threads (if any).

        Returns:
            None
        """
        self._stop_source()
        self._stop_housekeeping()
        self.as_consumer.leave()
        self.as_producer.leave()
        self._consumer = None
        self._producer = None

    @classmethod
    def get_or_create(cls, name, url=None, interval=None, batchsize=1, max_age=None,
                      **kwargs):
        """ get or create a stream

        Args:
            name (str): the name of the stream
            url (str): the database url
            interval (float): the interval in seconds, or a fraction thereof,
               DEPRECATED
            batchsize (int): the batch size, DEPRECATED
            max_age (float|int|dict): the maximum age of the data in seconds, or as a dict
                to datetime.timedelta(**kwargs). Specifies the interval to run the
                housekeeping thread and the maximum age of data in the buffer, relative
                to the created timestamp. If None, the housekeeping thread is stopped.

        Returns:
            Stream: the stream object
        """
        # critical section
        # this may fail in concurrency situations
        from minibatch import connectdb
        try:
            db_specs = connectdb(alias='minibatch', url=url, **kwargs)
        except Exception as e:
            warning("Stream setup resulted in {} {}".format(type(e), str(e)))
        else:
            logger.debug(f'Stream {name=} connected using {db_specs=}')
        try:
            stream = Stream.objects(name=name).no_cache().get()
        except Stream.DoesNotExist:
            try:
                stream = Stream(name=name or uuid4().hex,
                                interval=interval,
                                status=STATUS_OPEN).save()
            except NotUniqueError:
                pass
            stream = Stream.objects(name=name).no_cache().get()
        stream.batchsize = batchsize
        stream._max_age = max_age
        stream.autoclear(max_age)
        return stream

    def buffer(self, **kwargs):
        self.flush()
        return Buffer.objects.no_cache().filter(**{'stream': self.name, **kwargs})

    def window(self, **kwargs):
        self.flush()
        return Window.objects.no_cache().filter(**{'stream': self.name, **kwargs})

    def streaming(self, fn=None, **kwargs):
        """ returns a streaming function

        Args:
            fn (callable): optional, a window function. If not
               specified the streaming function is returned as a decorator
               to an actual window function.
            **kwargs: kwargs passed to minibatch.streaming()

        Returns:
            fn (callable): the streaming function
        """
        from minibatch import streaming as _base_streaming
        return _base_streaming(self.name, fn=fn, url=self._url, cnx_kwargs=self._cnx_kwargs, **kwargs)

    @property
    def source(self):
        return self._stream_source

    def autoclear(self, max_age=None):
        # specify max_age in seconds or as a dict to timedelta(**kwargs)
        # None means never clear
        self._max_age = max_age if max_age is not None else self._max_age
        self._start_housekeeping()

    def _housekeeping(self):
        # the actual housekeeping thread
        while self._max_age:
            max_age = self._max_age
            if not isinstance(max_age, dict):
                max_age = dict(seconds=max_age)
            earliest = datetime.datetime.utcnow() - datetime.timedelta(**max_age)
            try:
                count = Buffer.objects.no_cache().filter(**{'stream': self.name, 'created__lte': earliest}).delete()
            except Exception as e:
                hk_logger.warning(f"housekeeping for stream {self.name} failed: {e}")
            else:
                hk_logger.info(f"housekeeping for stream {self.name}: deleted {count} objects earlier than {earliest}")
            # effectively we keep at most 2x _max_age periods of data
            # -- example: max_age = 10
            #    t: ---------+---------+---------+
            #       0       10        20        30
            #       dddddddddd (d = data)
            #                * _housekeeping runs, deletes before t0
            #       dddddddddddddddddddd
            #                          * _housekeeping runs, deletes before t10
            #       xxxxxxxxxxdddddddddd (x = deleted)
            #                 dddddddddddddddddddd
            #                                      * _housekeeping runs, deletes before t20
            #                 xxxxxxxxxxddddddddddd (x = deleted)

            # we use this instead of sleep() to allow for a quick stop()
            # -- using sleep() means the thread waits up to max_age time (which could be very long, days, months, etc)
            # -- using Event.wait() means the thread waits up to max_age time, but can be stopped immediately by
            #    setting the event
            # -- see https://stackoverflow.com/a/42710697/890242
            # TODO refactor this into a context manager or decorator so we can easily reuse it
            if self._housekeeping_stop_ev.wait(timeout=datetime.timedelta(**max_age).total_seconds()):
                break
        hk_logger.debug(f"housekeeping for stream {self.name} stopped")

    def _start_source(self, source):
        try:
            self._source_thread = t = Thread(target=source.stream,
                                             args=(self,))
            t.start()
        except (KeyboardInterrupt, SystemExit):
            self.stop()

    def _stop_source(self):
        # stop source
        source = getattr(self, '_stream_source', None)
        if source:
            source.cancel()

    def _start_housekeeping(self):
        try:
            self._housekeeping_stop_ev = threading.Event()
            self._housekeeping_thread = t = Thread(target=self._housekeeping)
            t.start()
        except (KeyboardInterrupt, SystemExit):
            self._stop_housekeeping()

    def _stop_housekeeping(self):
        self._max_age = None
        self._housekeeping_stop_ev.set()

    @classmethod
    def shutdown(cls):
        # stop all instances of Stream
        import gc
        [obj.stop() for obj in gc.get_objects() if isinstance(obj, Stream)]


class Participant(Document):
    """ A participant in a stream

    Participants are used to enable multi-producer, multi-consumer scenarios.

    Each participant is uniquely identified by stream, role, and hostname, whereby
    hostname is a combination of the actual hostname, process id, and thread id. The
    role is either 'producer' or 'consumer'. The elector id is used to determine a leader
    among all participants in a stream. The leader is responsible for housekeeping and
    balancing the stream.

    Participants build a directed graph from producers to consumers, whereby each connection
    is called a chord. A chord is a unique identifier of a consumer, or a set of consumers.
    Upon inserting a message into the buffer, the producer selects a chord to send the message
    to. The chord can be chosen randomly or upon appending the message. In a multi-consumer
    scenario, chosing the chord randomly distributes all messages evenly across all consumers,
    effectively load balancing the stream. In case a participants becomes inactive after messages
    are appended to the stream the leader will assign the messages to a recently active chord.

    Guarantees:
        - messages not processed within a threshold will be rerouted to another active chord
        - to this end, all participants reach a consensus on leadership, so that there is always a leader

    Housekeeping:
        - housekeeping is done by the leader of a stream
        - housekeeping is done on a regular interval (Participant.ACTIVE_INTERVAL)
        - housekeeping deletes messages in the buffer that are older than a certain age
        - housekeeping balances the messages in the buffer across all chords (only leader)

    Stream balancing:
        - stream balancing is done by the leader of a stream
        - stream balancing assigns messages in the buffer to a recently active chord

    How leader election works:
        - a leader is chosen randomly by participants of a particular stream, with equal probability
        - the leader in a stream is the participant with the highest elector id in that stream
        - the elector id is chosen, at random, by each participant upon joining;
        - the dbms is used to guarantee uniqueness of elector ids
        - consensus is established eventually by every participant deciding leader status on an interval

    .. versionadded:: 0.6.0
    """
    stream = StringField(required=True)
    role = StringField(required=True)
    created = DateTimeField(default=datetime.datetime.utcnow)
    last_beat = DateTimeField(default=datetime.datetime.utcnow)
    chord = StringField(required=True, default='default')
    hostname = StringField(required=True, default=lambda: Participant.my_hostname())
    elector = IntField(required=True, default=lambda: randint(0, Participant.MAX_ELECTOR))
    meta = {
        'db_alias': 'minibatch',
        'strict': False,  # support previous releases
        'indexes': [
            'created',
            'chord',
            'hostname',
            {'fields': ['stream', 'elector'], 'unique': True},
        ]
    }

    ACTIVE_INTERVAL = 1  # seconds
    GRACE_PERIOD = 5  # seconds
    MAX_ELECTOR = 1000000
    MYSELF = ProcessLocal()
    STRATEGY = 'random'
    _key = lambda stream, role, hostname: f'{stream}_{role}_{hostname}'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.alive = ThreadAlive()
        self._all_chords = set()
        # the lock protects beat() and leave() to ensure atomicity
        # -- beat() and leave() are only called  by the actual instance of the participant
        # -- there is no distributed scenario where these methods are called by multiple threads/processes
        # -- if we don't use a lock, we end up seeing Participants(stream=None, role=None, hostname=None)
        self.lock = threading.Lock()

    def __repr__(self):
        leading = self.my_leading()
        return f"Participant({self.stream=},{self.role=},{self.hostname=},{self.chord=},{self.active=},{leading=})"

    @classmethod
    def leader(cls, stream):
        stream = stream.name if isinstance(stream, Stream) else stream
        since = cls._last_active(cls)
        return Participant.objects(stream=stream, last_beat__gte=since).order_by('-elector').limit(1).first()

    @property
    def last_active(self):
        return self._last_active(self)

    @staticmethod
    def _last_active(self):
        grace_period = datetime.timedelta(seconds=self.GRACE_PERIOD)
        return datetime.datetime.utcnow() - (datetime.timedelta(seconds=self.ACTIVE_INTERVAL) + grace_period)

    def my_leading(self):
        leader = Participant.leader(self.stream)
        return leader.pk == self.pk if leader else False

    def beat(self):
        # acquiring the lock to avoid interference with start() and leave()
        with self.lock:
            if not self.alive:
                return
            self.last_beat = datetime.datetime.utcnow()
            self.modify(chord=self.chord, last_beat=self.last_beat)

    def start(self):
        # acquiring the lock to avoid interference with leave() from another thread
        with self.lock:
            if self.alive and not self.alive.thread:
                logger.debug(f"starting {self!r}")
                t = self.alive.thread = Thread(target=self._run_beat_thread)
                t.start()

    def select_chord(self):
        return random.choice(self.chords) if self.STRATEGY == 'random' else self.chord

    @property
    def chords(self):
        return list(self._all_chords) or [self.chord]

    def _run_beat_thread(self):
        while self.alive:
            try:
                logger.debug(f"beat {self!r}")
                self.beat()
                self.housekeep()
                self.alive.wait(self.ACTIVE_INTERVAL)
            except Exception as e:
                logger.error(e)

    def leave(self, wait=False, timeout=5):
        # acquiring the lock to avoid interference with beat()
        with self.lock:
            logger.debug(f"leaving {self!r}")
            # stop this instance and delete from registry db
            # -- this could come from a Participant.for_stream() query
            self.alive.stop(wait=wait, timeout=timeout)
            self.delete()
            # stop any other instance for this Participant
            # -- this could come from a Participant.register() call
            # -- if we don't do this we might end up with beat() threads keep running
            #    (rationale: .leave() may be called interactively/from a script, deleting the participant but
            #     not stopping the beat() thread. We avoid this by stopping the thread here)
            part: Participant | None = self.MYSELF.pop(Participant._key(self.stream, self.role, self.hostname), None)
            part.alive.stop(wait=wait, timeout=timeout) if part else None

    def housekeep(self, since=None):
        # cleanup known chords
        since = since if since is not None else self.last_active
        active = Participant.objects(stream=self.stream, last_beat__gte=since).no_cache()
        inactive = Participant.objects(stream=self.stream, last_beat__lt=since).no_cache()
        self._all_chords = set(active.filter(role='consumer').distinct('chord'))
        self._all_chords -= set(inactive.filter(role='consumer').distinct('chord'))
        if self.my_leading():
            self.leader_housekeep(since, inactive)

    def leader_housekeep(self, since, inactive):
        # remove inactive participants
        inactive.delete()
        # assign default messages
        messages = Buffer.objects(stream=self.stream, chord='default', processed=False).order_by('created')
        for msg in messages:
            msg.update(chord=self.select_chord())
        # balance stream
        messages = Buffer.objects(stream=self.stream, created__lt=since, processed=False).order_by('chord')
        for g, msgs in groupby(messages, key=lambda m: m.chord):
            # messages in previously the same chord should stay in the same chord
            new_chord = self.select_chord()
            for msg in msgs:
                msg.update(chord=new_chord)

    @classmethod
    def get_or_create(cls, stream, role, hostname=None, chord=None):
        hostname = hostname or Participant.my_hostname()
        participant = None
        errors = []
        # for consumers, we always set a chord (these are fixed for the lifetime of the consumer)
        # for producers, we select a random chord (distribute evenly)
        default_chord = uuid4().hex if role == 'consumer' else 'default'
        chord = chord or default_chord
        try:
            participant = Participant.objects(stream=stream, role=role, hostname=hostname).no_cache().get()
        except (DoesNotExist, DuplicateKeyError) as e:
            retry = 5
            while retry:
                try:
                    participant = Participant(stream=stream, role=role, hostname=hostname, chord=chord).save()
                except NotUniqueError as e:
                    # we retry a few times to set a new elector
                    retry -= 1
                    errors.append(e)
                else:
                    retry = 0
        assert participant is not None, f"could not create Participant({stream=},{role=},{hostname=}) due to {errors}"
        return participant

    @classmethod
    def register(cls, stream, role, hostname=None, chord=None):
        participant = cls.get_or_create(stream, role, hostname=hostname, chord=chord)
        participant.start()
        partname = cls._key(stream, role, hostname)
        cls.MYSELF[partname] = participant
        return participant

    @classmethod
    def for_stream(cls, stream, role=None):
        filter = {'stream': stream.name if isinstance(stream, Stream) else stream}
        filter.update(role=role) if role else None
        return list(cls.objects(**filter).no_cache())

    @classmethod
    def producers(cls, stream):
        return cls.for_stream(stream, role='producer')

    @classmethod
    def consumers(cls, stream):
        return cls.for_stream(stream, role='consumer')

    @classmethod
    def myself(cls, stream, role, hostname=None):
        stream = stream.name if isinstance(stream, Stream) else stream
        hostname = hostname or Participant.my_hostname()
        partname = cls._key(stream, role, hostname)
        if cls.MYSELF.get(partname) is None:
            participant = cls.register(stream, role, hostname=hostname)
        return cls.MYSELF[partname]

    @property
    def active(self):
        return self.last_beat >= self.last_active

    @classmethod
    def my_hostname(cls):
        return f'{socket.gethostname()}-{os.getpid()}-{threading.get_native_id()}'

    @classmethod
    def shutdown(cls):
        logger.info("Shutting down all participants and streams")
        for part in list(cls.MYSELF.values()):
            part.leave(wait=True)


class Monitor:
    def __init__(self):
        Participant.myself('system', 'monitor')

    def participants(self):
        for s in Stream.objects.no_cache():
            Participant.myself(s.name, 'monitor')
            for p in Participant.for_stream(s):
                yield p


def shutdown():
    Participant.shutdown()
    Stream.shutdown()


atexit.register(shutdown)
