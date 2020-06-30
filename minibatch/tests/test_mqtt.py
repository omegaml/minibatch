import json
from unittest import TestCase

from time import sleep
from unittest.mock import MagicMock

from minibatch import connectdb, stream, reset_mongoengine
from minibatch.contrib.mqtt import MQTTSource, MQTTSink
from minibatch.tests.util import delete_database


class MQTTTests(TestCase):
    def setUp(self):
        self.url = 'mongodb://localhost/test'
        delete_database(url=self.url)
        self.db = connectdb(url=self.url)

    def tearDown(self):
        reset_mongoengine()

    def test_source(self):
        # we simply inject a mock MQTTClient into the MQTTSource
        source = MQTTSource('localhost', 'TEST/#')
        client = MagicMock()
        client.loop_forever = lambda *args: sleep(10)
        source._client = client
        s = stream('test', url=self.url)
        s.attach(source)
        s.append = MagicMock()
        message = MagicMock()
        message.payload = json.dumps({'foo': 'bar'}).encode('utf-8')
        source.on_message(client, {}, message)
        s.append.assert_called()
        s.stop()

    def test_sink(self):
        # we simply inject a mock MQTTClient into the MQTTSource
        sink = MQTTSink('localhost', 'TEST/#')
        client = MagicMock()
        client.loop_forever = lambda *args: sleep(10)
        sink._client = client
        sink.put({'foo': 'bar'})
        client.publish.assert_called_with('TEST/#', json.dumps(dict(foo='bar')))
