import pymongo
from time import sleep

from minibatch import logger


class MongoSource:
    """
    A mongodb collection source

    Usage:
      # start consuming from mongo collection
      stream = mb.stream('test')
      source = MongoSource(collection)
      stream.attach(source)

      # stream to a python callable
      streaming('test')(lambda v: print(v))

    Args:
        collection (pymongo.Collection): a mongo collection
        size (int): the number of new documents to fetch for each stream.append
            defaults to 1
        idcol (str): the name of the id column, defaults to _id
        delay (float): the wait time in seconds between change queries, defaults
           to .1

    Notes:
        * the collection must have a key column that is naturally ordered (ascending),
          that is a new records' key must be compare greater as any previous key

        * by default MongoSources uses the object id (_id column) as the sort order,
          because it is naturally increasing for each new inserted object. Specify
          as idcol='column name'

        * MongoSource implements a polling change observer that is executed on
          once every delay seconds, for every query it retrieves at most N=size
          messages. All messages are appended to the stream one by one.
          If you know that new messages arrive more or less frequently change
          either size or delay to optimize polling behavior. For example if
          messages arrive more frequently than .1 seconds but processing them
          in steps of > .1 seconds is ok, specify size=number of messages in each
          interval. If messages arrive less frequently than every .1 seconds,
          considering specifying a delay > .1 seconds to reduce the polling load
          on the database.

        * For a Mongo replicaset, use the MongoReplicasetSource instead. It uses
          the MongoDB native change stream instead of a polling change observer
          which is more efficient.
    """

    def __init__(self, collection, size=1, idcol=None, delay=.1):
        self.collection = collection
        self._cancel = None
        self._lastid = None
        self._size = size
        self._idcol = '_id'
        self._delay = delay

    def changes(self, N=1):
        latest_id = None
        while not self._cancel:
            sortkey = {
                'sort': [('_id', pymongo.ASCENDING)],
            }
            query = {}
            if latest_id is not None:
                query[self._idcol] = {
                    '$gt': latest_id
                }
            docs = self.collection.find(query, **sortkey).limit(N)
            for doc in docs:
                latest_id = doc[self._idcol]
                yield doc
            sleep(self._delay)

    def stream(self, stream):
        self._cancel = False
        while not self._cancel:
            for doc in self.changes(N=self._size):
                if self._cancel:
                    break
                stream.append(doc)
        logger.debug("stream done")

    def cancel(self):
        self._cancel = True


class MongoSink:
    """
    A mongodb collection sink
    """
    def __init__(self, collection):
        self.collection = collection

    def put(self, messages):
        if isinstance(messages, dict):
            messages = [messages]
        return self.collection.insert_many(messages)


class MongoReplicasetSource(MongoSource):
    def changes(self, N=1):
        criteria = [
            {'$match': {
                'operationType': 'insert'
            }}
        ]
        self._cancel = False
        docs = []
        with self.collection.watch(criteria) as changes:
            for doc in changes:
                if self._cancel:
                    break
                docs.append(doc)
                if len(docs) >= N:
                    yield docs
                    docs = []
