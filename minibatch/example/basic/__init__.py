# some worker's process function

from minibatch import connectdb, Stream, streaming
from minibatch.example.util import clean


def consumer():
    # process window.data. maybe split processing in parallel... whatever
    # @stream('test', size=2, emitter=SampleFunctionWindow)
    # @stream('test', interval=5)
    # @stream('test', interval=5, relaxed=False, keep=True)
    @streaming('test', size=5, keep=True)
    def myprocess(window):
        try:
            db = connectdb(alias='consumer')
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
    connectdb(alias='producer')
    stream_name = 'test'
    stream = Stream.get_or_create(stream_name)
    print("producing ... {}".format(data))
    stream.append(data)


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
    db = connectdb()
    print("processed items:")
    print(list(doc for doc in db.processed.find()))
