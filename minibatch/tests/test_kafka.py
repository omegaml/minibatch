from multiprocessing import Process, Queue
from unittest import TestCase

from threading import Thread
from time import sleep
from unittest.mock import MagicMock

from minibatch import connectdb, stream, streaming, make_emitter
from minibatch.contrib.kafka import KafkaSource, KafkaSink
from minibatch.tests.util import delete_database, LocalExecutor


class KafkaTests(TestCase):
    def setUp(self):
        self.url = 'mongodb://localhost/test'
        delete_database(url=self.url)
        self.db = connectdb(url=self.url)

    def test_consumer(self):
        # we simply inject a mock KafkaConsumer into the KafkaSource
        # as we don't want to test KafkaConsumer but KafkaSource
        message = MagicMock()
        message.value = dict(foo='bar')
        source = KafkaSource('topic')
        consumer = MagicMock()
        consumer.__iter__.return_value = [message]
        source._consumer = consumer
        s = stream('test', url=self.url)
        s.attach(source)

        def consumer(q):
            url = str(self.url)

            @streaming('test', executor=LocalExecutor(), url=url, queue=q)
            def process(window):
                db = connectdb(url=url)
                db.processed.insert_many(window.data)

        q = Queue()
        p = Process(target=consumer, args=(q,))
        p.start()
        sleep(5)
        q.put(True)
        p.join()

        docs = list(self.db.processed.find())
        self.assertEqual(len(docs), 1)

    def test_sink(self):
        # we simply inject a mock KafkaProducer into the KafkaSink
        s = stream('test', url=self.url)
        s.append(dict(foo='baz'))
        sink = KafkaSink('test')
        producer = MagicMock()
        sink._producer = producer
        # create a threaded emitter that we can stop
        em = make_emitter('test', url=self.url, sink=sink, emitfn=lambda v: v)
        t = Thread(target=em.run)
        t.start()
        sleep(1)
        em._stop = True
        # check the  sink got called and forward to the mock KafkaProducer
        producer.send.assert_called_with('test', value={'foo': 'baz'})
