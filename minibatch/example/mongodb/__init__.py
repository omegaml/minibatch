from datetime import datetime
from multiprocessing import Process

from time import sleep

from minibatch import connectdb, streaming, stream
from minibatch.contrib.mongodb import MongoSource, MongoSink
from minibatch.example.util import clean


def consumer():
    @streaming('test', size=1, keep=True)
    def myprocess(window):
        try:
            db = connectdb(alias='consumer')
            print("consuming ... {}".format(window.data))
            db.processed.insert_one({'data': window.data or {}})
        except Exception as e:
            print(e)
        return window


def main():
    print("setting up")
    clean()
    # setup mqtt source and producer
    url = 'mongodb://localhost/test'
    db = connectdb(url=url)
    source_coll = db['source']
    sink_coll = db['processed']
    source = MongoSource(source_coll)
    producer = MongoSink(sink_coll)
    # attach to the stream
    s = stream('test')
    s.attach(source)
    # set up a streaming function
    emitp = Process(target=consumer)
    emitp.start()
    # publish some messages
    print("publishing messages")
    for i in range(10):
        producer.put(dict(foo='bar', time=datetime.utcnow().isoformat()))
        sleep(.1)
    # check we got the messages
    print("wait to receive all messages")
    sleep(3)
    docs = list(doc for doc in sink_coll.find())
    print("processed items:", len(docs))
    print(docs)
    emitp.terminate()
