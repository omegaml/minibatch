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


class Window(Document):
    """
    A Window is the data collected from a stream according
    to the WindowEmitter strategy.
    """
    stream = StringField(required=True)
    created = DateTimeField(default=datetime.datetime.now)
    data = ListField(default=[])
    processed = BooleanField(default=False)
    meta = {
        'db_alias': 'minibatch',
        'indexes': [
            'created',
            'stream',
        ]
    }

    def __unicode__(self):
        return u"Window [%s] %s" % (self.created, self.data)


class Buffer(Document):
    stream = StringField(required=True)
    created = DateTimeField(default=datetime.datetime.now)
    data = DictField(required=True)
    processed = BooleanField(default=False)
    meta = {
        'db_alias': 'minibatch',
        'indexes': [
            'created',
            'stream',
        ]
    }

    def __unicode__(self):
        return u"Buffer [%s] %s" % (self.created, self.data)


class Stream(Document):
    """
    Stream provides meta data for a streaming buffer

    Streams are synchronized among multiple Stream clients using last_read.
    """
    name = StringField(default=lambda: uuid4().hex, required=True)
    status = StringField(choices=STATUS_CHOICES, default=STATUS_INIT)
    created = DateTimeField(default=datetime.datetime.now)
    closed = DateTimeField(default=None)
    # interval in seconds or count in #documents
    interval = IntField(default=10)
    last_read = DateTimeField(default=datetime.datetime.now)
    meta = {
        'db_alias': 'minibatch',
        'indexes': [
            'created',  # most recent is last, i.e. [-1]
            {'fields': ['name'],
             'unique': True
             }
        ]
    }

    def ensure_initialized(self):
        if self.status == STATUS_INIT:
            self.modify({'status': STATUS_INIT},
                        status=STATUS_OPEN)

    def append(self, data):
        """
        non-blocking append to stream buffer
        """
        self.ensure_initialized()
        Buffer(stream=self.name,
               data=data).save(write_concern=dict(w=0, j=False))

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
    def get_or_create(cls, name, url=None, **kwargs):
        # critical section
        # this may fail in concurrency situations
        from minibatch import connectdb
        try:
            connectdb(alias='minibatch', url=url)
        except Exception as e:
            warning("Stream setup resulted in {} {}".format(type(e), str(e)))
        try:
            stream = Stream.objects(name=name).no_cache().get()
        except Stream.DoesNotExist:
            pass
        try:
            stream = Stream(name=name or uuid4().hex,
                            status=STATUS_OPEN,
                            **kwargs).save()
        except NotUniqueError:
            stream = Stream.objects(name=name).no_cache().get()
        return stream
