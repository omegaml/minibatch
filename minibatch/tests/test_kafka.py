from multiprocessing import Process
from time import sleep
from unittest import TestCase

from unittest.mock import MagicMock

from minibatch import connectdb, stream, streaming
from minibatch.contrib.kafka import KafkaSource
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
        s = stream('test')
        s.attach(source)

        def consumer():
            url = str(self.url)

            @streaming('test', executor=LocalExecutor())
            def process(window):
                db = connectdb(url=url)
                db.processed.insert(window.data)

        p = Process(target=consumer)
        p.start()
        sleep(1)
        p.terminate()

        docs = list(self.db.processed.find())
        self.assertEqual(len(docs), 1)
