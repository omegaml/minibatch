# some worker's process function
from mongoengine.connection import disconnect

from minibatch import setup, Stream, streaming


def consumer():
    # process window.data. maybe split processing in parallel... whatever
    # @stream('test', size=2, emitter=SampleFunctionWindow)
    # @stream('test', interval=5)
    # @stream('test', interval=5, relaxed=False, keep=True)
    @streaming('test', size=5, keep=True)
    def myprocess(window):
        try:
            db = setup(alias='consumer')
            print("consuming ... {}".format(window.data))
            db.processed.insert_one({'data': window.data or {}})
        except Exception as e:
            print(e)
        return window


# some producer
def producer(data):
    import os
    import time
    import random
    # sleep to simulate multiple time windows
    time.sleep(random.randrange(0, 1, 1) / 10.0)
    data.update({'pid': os.getpid()})
    db = setup(alias='producer')
    stream_name = 'test'
    stream = Stream.get_or_create(stream_name)
    print("producing ... {}".format(data))
    stream.append(data)

def clean():
    db = setup()
    db.drop_collection('buffer')
    db.drop_collection('stream')
    db.drop_collection('window')
    db.drop_collection('processed')
    disconnect('minibatch')

def main():
    from multiprocessing import Pool, Process
    import time

    clean()
    emitp = Process(target=consumer)
    emitp.start()
    pool = Pool(4)
    data = [{'value': i} for i in range(0, 100)]
    pool.map(producer, data, 1)
    time.sleep(5)
    emitp.terminate()
    db = setup()
    print("processed items:")
    print(list(doc for doc in db.processed.find()))
