from logging import warning

import datetime
import logging
import threading
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

# we don't propagate this logger to avoid logging housekeeping messages unless requested
hk_logger = logging.getLogger(__name__ + '.housekeeping')
hk_logger.propagate = False


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

    def __init__(self, *args, batchsize=1, max_age=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._batcher = None
        self._url = None
        self._cnx_kwargs = None
        self._stream_source = None
        self.batchsize = batchsize
        self._max_age = max_age
        self.ensure_initialized()
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

    def append(self, data):
        t = datetime.datetime.utcnow()
        Buffer.write(dict(stream=self.name, data=data or {}, processed=False, created=t), batcher=self._batcher)

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

    def stop(self):
        """ stop the stream

        Stops the source and housekeeping threads (if any).

        Returns:
            None
        """
        self._stop_source()
        self._stop_housekeeping()

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
            max_age (int|float|dict): the maximum age of the data in seconds, or as a dict
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
        stream._url = url
        stream._cnx_kwargs = kwargs
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
