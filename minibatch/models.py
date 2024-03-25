from contextlib import contextmanager

import random

import os

import threading
from logging import warning
from time import sleep

import datetime
import socket
from mongoengine import Document
from mongoengine.errors import NotUniqueError
from mongoengine.fields import (StringField, IntField, DateTimeField,
                                ListField, DictField, BooleanField)
from random import randint
from threading import Thread
from uuid import uuid4

STATUS_INIT = 'initialize'
STATUS_OPEN = 'open'
STATUS_CLOSED = 'closed'
STATUS_PROCESSED = 'processed'
STATUS_FAILED = 'failed'
STATUS_CHOICES = (STATUS_OPEN, STATUS_CLOSED, STATUS_FAILED)


class ThreadAlive(threading.Event):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.thread = None

    def __bool__(self):
        return not self.is_set()

    def stop(self):
        self.set()
        self.thread.join(1) if self.thread else None
        self.thread = None


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
        if batcher is None:
            cls._get_collection().insert_one(doc)
        else:
            batcher.add(doc)
            if batcher.is_full:
                cls.flush(batcher)

    @classmethod
    def flush(cls, batcher=None):
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
        return u"Buffer created=[%s] processed=%s data=%s" % (self.created, self.processed, self.data)


class Stream(Document):
    """
    Stream provides meta data for a streaming buffer

    Streams are synchronized among multiple Stream clients using last_read.
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

    def __init__(self, *args, batchsize=1, **kwargs):
        super().__init__(*args, **kwargs)
        self.ensure_initialized()
        self._batcher = None
        self.batchsize = batchsize

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
        return Participant.myself(self.name, 'producer')

    @property
    def as_consumer(self):
        return Participant.myself(self.name, 'consumer')

    def append(self, data, chord=None):
        t = datetime.datetime.utcnow()
        chord = chord or self.as_producer.select_chord()
        Buffer.write(dict(stream=self.name, chord=chord,
                          data=data or {}, processed=False, created=t), batcher=self._batcher)

    def flush(self):
        Buffer.flush(self._batcher)

    def attach(self, source, background=True):
        """
        use an external producer to start streaming
        """
        self._stream_source = source
        self.as_producer.start()
        if not background:
            source.stream(self)
        else:
            self._source_thread = t = Thread(target=source.stream,
                                             args=(self,))
            t.start()

    def stop(self):
        source = getattr(self, '_stream_source', None)
        if source:
            source.cancel()

    @classmethod
    def get_or_create(cls, name, url=None, interval=None, batchsize=1, **kwargs):
        # critical section
        # this may fail in concurrency situations
        from minibatch import connectdb
        try:
            connectdb(alias='minibatch', url=url, **kwargs)
        except Exception as e:
            warning("Stream setup resulted in {} {}".format(type(e), str(e)))
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
        return stream

    def buffer(self, **kwargs):
        return Buffer.objects.no_cache().filter(**{'stream': self.name, **kwargs})

    def window(self, **kwargs):
        return Window.objects.no_cache().filter(**{'stream': self.name, **kwargs})


class Participant(Document):
    stream = StringField(required=True)
    role = StringField(required=True)
    created = DateTimeField(default=datetime.datetime.utcnow)
    last_beat = DateTimeField(default=datetime.datetime.utcnow)
    chord = StringField(required=True, default='default')
    hostname = StringField(required=True, default=lambda: Participant.my_hostname())
    elector = IntField(default=lambda: randint(0, 1000000))
    meta = {
        'db_alias': 'minibatch',
        'strict': False,  # support previous releases
        'indexes': [
            'created',
            {'fields': ['stream', 'elector'], 'unique': True},
        ]
    }

    ACTIVE_INTERVAL = 10  # seconds
    MYSELF = threading.local()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.alive = ThreadAlive()
        self._all_chords = []

    @classmethod
    def leader(cls, stream):
        since = datetime.datetime.utcnow() - datetime.timedelta(seconds=cls.ACTIVE_INTERVAL)
        return Participant.objects(stream=stream, last_beat__gte=since).order_by('-elector').first()

    def my_leading(self, hostname=None):
        hostname = hostname or socket.gethostname()
        return Participant.leader(self.stream).hostname == hostname

    def beat(self):
        self.update(last_beat=datetime.datetime.utcnow())

    def start(self):
        if not self.alive.thread:
            t = self.alive.thread = Thread(target=self._run_beat_thread)
            t.start()

    def stop(self):
        self.alive.stop()

    def select_chord(self):
        if self.chord == 'default':
            self.update(chord=random.choice(self.chords))
        return self.chord

    @property
    def chords(self):
        return self._all_chords or [self.chord]

    def _run_beat_thread(self):
        while self.active:
            self.beat()
            self.housekeep()
            self.alive.wait(self.ACTIVE_INTERVAL)

    def leave(self):
        self.stop()
        self.delete()

    def housekeep(self):
        # clean up inactive participants
        since = datetime.datetime.utcnow() - datetime.timedelta(seconds=self.ACTIVE_INTERVAL)
        Participant.objects(last_beat__lt=since).delete()
        # balance the buffer
        # find all active chords
        self._all_chords = list(Participant.objects(stream=self.stream).no_cache().distinct('chord'))

    @classmethod
    def get_or_create(cls, stream, role, hostname=None, chord=None):
        hostname = hostname or Participant.my_hostname()
        participant = None
        errors = []
        try:
            participant = Participant.objects(stream=stream, role=role, hostname=hostname).no_cache().get()
        except Participant.DoesNotExist as e:
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
        hostname = hostname or Participant.my_hostname()
        # for consumers, we always set a chord (these are fixed for the lifetime of the consumer)
        # for producers, we select a random chord (distribute evenly)
        default_chord = uuid4().hex if role == 'consumer' else 'default'
        chord = chord or default_chord
        participant = cls.get_or_create(stream, role, hostname=hostname, chord=chord)
        participant.start()
        return participant

    @classmethod
    def for_stream(cls, stream, role=None):
        filter = {'stream': stream}
        filter.update(role=role) if role else None
        return cls.objects(**filter).no_cache()

    @classmethod
    def producers(cls, stream):
        return cls.for_stream(stream, role='producer')

    @classmethod
    def consumers(cls, stream):
        return cls.for_stream(stream, role='consumer')

    @classmethod
    def myself(cls, stream, role, hostname=None):
        partname = f'{stream}_{role}'
        if not hasattr(cls.MYSELF, partname):
            hostname = hostname or Participant.my_hostname()
            participant = cls.register(stream, role, hostname=hostname)
            setattr(cls.MYSELF, partname, participant)
        return getattr(cls.MYSELF, partname)

    @property
    def active(self):
        return self.last_beat > datetime.datetime.utcnow() - datetime.timedelta(seconds=self.ACTIVE_INTERVAL)

    @classmethod
    def my_hostname(cls):
        return socket.gethostname() + '-' + str(os.getpid())
