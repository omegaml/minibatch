import datetime
from threading import Thread
from uuid import uuid4
import enum
import json

from sqlalchemy.orm import as_declarative, declared_attr, synonym
from sqlalchemy.sql import func
from sqlalchemy import MetaData, Index, Column, String, Boolean, text, DateTime, Enum, Integer, Text

from minibatch.models import Batcher, STATUS_INIT, STATUS_OPEN, STATUS_CLOSED, STATUS_PROCESSED, STATUS_FAILED


class StatusChoices(enum.Enum):
    status_open = STATUS_OPEN
    status_closed = STATUS_CLOSED
    status_failed = STATUS_FAILED
    status_init = STATUS_INIT


@as_declarative(metadata=MetaData(schema='minibatch'))
class Base:

    def modify(self, **attributes):
        from minibatch import db_session
        for attr, value in attributes.items():
            setattr(self, attr, value)
        db_session.merge(self)
        db_session.flush()
        db_session.commit()


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
        from minibatch import db_session

        if batcher is None:
            db_session.bulk_insert_mappings(cls, [doc])
            db_session.commit()
        else:
            batcher.add(doc)
            if batcher.is_full:
                cls.flush(batcher)

    @classmethod
    def flush(cls, batcher=None):
        if batcher is not None:
            from minibatch import db_session
            # the iterable is to avoid duplicate objects (batch op errors)
            db_session.bulk_insert_mappings(cls, [dict(d) for d in batcher.batch])
            batcher.clear()


class Window(ImmediateWriter, Base):
    """
    A Window is the data collected from a stream according
    to the WindowEmitter strategy.
    """
    __tablename__ = 'windows'

    stream = Column(String, nullable=False)
    created = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    _data = Column('data', Text)
    processed = Column(Boolean, nullable=False, default=text('false'))
    _query = Column('query', Text)

    def get_data(self):
        return json.loads(self._data) if self._data else {}

    def set_data(self, value):
        if value:
            self._data = json.dumps(value)

    @declared_attr
    def data(cls):
        return synonym('_data', descriptor=property(cls.get_config, cls.set_config))

    def get_query(self):
        return json.loads(self._query) if self._query else {}

    def set_query(self, value):
        if value:
            self._query = json.dumps(value)

    @declared_attr
    def query(cls):
        return synonym('_data', descriptor=property(cls.get_query, cls.set_query))

    __table_args__ = (Index('minibatch_window_stream_created', "stream", "created"))

    def __unicode__(self):
        return u"Window [%s] %s" % (self.created, self.data)

    @classmethod
    def get_by_stream(cls, name, **kwargs):
        from minibatch import db_session
        return db_session.query(Window).find(**{'stream': name, **kwargs}).all()


class Buffer(ImmediateWriter, Base):
    __tablename__ = 'buffers'

    stream = Column(String, nullable=False)
    created = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    _data = Column('data', Text)
    processed = Column(Boolean, nullable=False, default=text('false'))

    def get_data(self):
        return json.loads(self._data) if self._data else {}

    def set_data(self, value):
        if value:
            self._data = json.dumps(value)

    @declared_attr
    def data(cls):
        return synonym('_data', descriptor=property(cls.get_config, cls.set_config))

    __table_args__ = (Index('minibatch_window_stream_created', "stream", "created"))

    def __unicode__(self):
        return u"Buffer created=[%s] processed=%s data=%s" % (self.created, self.processed, self.data)

    @classmethod
    def get_by_stream(cls, name, **kwargs):
        from minibatch import db_session
        return db_session.query(Buffer).find(**{'stream': name, **kwargs}).all()


class Stream(Base):
    """
    Stream provides meta data for a streaming buffer

    Streams are synchronized among multiple Stream clients using last_read.
    """
    __tablename__ = 'streams'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, default=lambda: uuid4().hex, nullable=False, unique=True, index=True)
    status = Column(Enum(StatusChoices), server_default=STATUS_INIT)
    created = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), index=True)
    closed = Column(DateTime(timezone=True))
    # interval in seconds or count in #documents
    interval = Column(Integer, default=10)
    last_read = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    def __init__(self, *args, batchsize=1, **kwargs):
        super().__init__(*args, **kwargs)
        self.ensure_initialized()
        self._batcher = None
        self.batchsize = batchsize

    def ensure_initialized(self):
        if self.status == STATUS_INIT:
            self.modify(status=STATUS_OPEN)

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
    def get_or_create(cls, name, interval=None, batchsize=1):
        # critical section
        # this may fail in concurrency situations
        from minibatch import db_session
        stream = db_session.query(Stream).filter(name=name).first()
        if not stream:
            try:
                stream = Stream(name=name or uuid4().hex,
                                interval=interval,
                                status=STATUS_OPEN)
                db_session.add(stream)
                db_session.commit()
            except:
                db_session.rollback()
                raise
            finally:
                cls.session.remove()
        stream.batchsize = batchsize
        return stream

    def buffer(self, **kwargs):
        from minibatch import db_session
        return db_session.query(Buffer).find(**{'stream': self.name, **kwargs}).all()

    def window(self, **kwargs):
        from minibatch import db_session
        return db_session.query(Buffer).find(**{'stream': self.name, **kwargs}).all()
