from datetime import datetime
from multiprocessing import Process, Queue
from threading import Thread
from unittest import TestCase

from time import sleep

from minibatch import connectdb, streaming, stream, make_emitter, Buffer, reset_mongoengine
from minibatch.contrib.mongodb import MongoSource, MongoSink
from minibatch.tests.util import delete_database, LocalExecutor


class MongodbTests(TestCase):
    def setUp(self):
        self.url = 'mongodb://localhost/test'
        delete_database(url=self.url)
        self.db = connectdb(url=self.url)

    def tearDown(self):
        reset_mongoengine()

    def test_source(self):
        N = 1
        interval = 1
        docs = self._run_streaming_test(N, interval, timeout=10)
        self.assertEqual(len(docs), N)

    def test_large_dataset_source(self):
        N = 100
        interval = 10
        docs = self._run_streaming_test(N, interval, timeout=15)
        self.assertEqual(len(docs), interval)
        print(list(d['delta'] for d in docs))

    def test_xlarge_dataset_source(self):
        # assert that large N do not cause the window processing time to grow
        N = 1000
        interval = 1000
        docs = self._run_streaming_test(N, interval, timeout=60)
        self.assertEqual(len(docs), N / interval)
        t_delta = sum(d['delta'] for d in docs) / len(docs)
        # we expect to see 0.5 seconds per batch, average
        # t_delta is in microseconds => 1e6 convert to seconds
        self.assertTrue(t_delta / 1e6 < 1.0)  # t_delta is in microseconds

    def test_sink(self):
        # we simply inject a mock KafkaProducer into the KafkaSink
        s = stream('test', url=self.url)
        s.append(dict(foo='baz'))
        db = self.db
        sink_coll = db['processed']
        sink = MongoSink(sink_coll)
        em = make_emitter('test', url=self.url, sink=sink, emitfn=lambda v: v)
        t = Thread(target=em.run)
        t.start()
        sleep(1)
        em._stop = True
        docs = list(sink_coll.find())
        self.assertEqual(len(docs), 1)

    def _run_streaming_test(self, N, interval, timeout=10):
        # set up a source collection that we want to steram
        coll = self.db['test']
        source = MongoSource(coll, size=N)
        # attach to the stream
        s = stream('test', url=self.url)
        s.attach(source)

        # stream consumer
        def consumer(q, interval):
            url = str(self.url)

            @streaming('test', size=interval, executor=LocalExecutor(), url=url, queue=q)
            def process(window):
                db = connectdb(url=url)
                # calculate average time t_delta it took for documents to be received since insertion
                dtnow = datetime.utcnow()
                t_delta = sum((dtnow - doc['dt']).microseconds for doc in window.data) / len(window.data)
                db.processed.insert_one(dict(delta=t_delta))

        # give it some input
        q = Queue()
        p = Process(target=consumer, args=(q, interval))
        p.start()

        for x in range(0, N, interval):
            docs = [{
                'foo': 'bar',
                'dt': datetime.utcnow()
            } for i in range(interval)]
            coll.insert_many(docs)
            sleep(1)

        sleep(timeout)
        s.stop()
        q.put(True)
        p.terminate()

        # check buffer is empty
        buffered_docs = list(Buffer.objects.filter())
        self.assertEqual(len(buffered_docs), 0)

        # return processed docs (in sink)
        docs = list(self.db.processed.find())
        return docs
