from logging import warning

import datetime
from mongoengine import Document
from mongoengine.errors import NotUniqueError
from mongoengine.fields import (StringField, IntField, DateTimeField,
                                ListField, DictField, BooleanField)
from threading import Thread
from uuid import uuid4

STATUS_INIT = 'initialize'
STATUS_OPEN = 'open'
STATUS_CLOSED = 'closed'
STATUS_PROCESSED = 'processed'
STATUS_FAILED = 'failed'
STATUS_CHOICES = (STATUS_OPEN, STATUS_CLOSED, STATUS_FAILED)


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

    def append(self, data):
        t = datetime.datetime.utcnow()
        Buffer.write(dict(stream=self.name, data=data or {}, processed=False, created=t), batcher=self._batcher)

    def flush(self):
        Buffer.flush(self._batcher)

    def attach(self, source, background=True):
        """
        use an external producer to start streaming
        """
        self._stream_source = source
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
