import json
from uuid import uuid4

import socket

import paho.mqtt.client as mqtt
from urllib.parse import urlparse


class MQTTNode:
    # based on https://pypi.org/project/paho-mqtt/
    mqtt_kind = 'node'

    def __init__(self, url, topic, **kwargs):
        self.topic = topic
        self.url = url
        self._stream = []
        self._client = None
        self._client_kwargs = dict(kwargs)
        if 'client_id' not in kwargs:
            client_id = '{}.{}.{}'.format(socket.gethostname(),
                                          self.mqtt_kind, uuid4().hex)
            self._client_kwargs['client_id'] = client_id
        if 'clean_session' not in kwargs:
            self._client_kwargs['clean_session'] = True

    @property
    def client(self):
        if self._client is None:
            client = self._client = mqtt.Client(**self._client_kwargs)
            client.on_connect = self.on_connect
            client.on_message = self.on_message
        return self._client

    def on_connect(self, client, userdata, flags, rc):
        pass

    def on_message(self, client, userdata, message):
        message.payload = json.loads(message.payload)

    def connect(self):
        client = self.client
        parsed = urlparse(self.url)
        client.username_pw_set(parsed.username, parsed.password)
        client.connect(parsed.hostname, port=parsed.port or 1883)

    def disconnect(self):
        self.client.disconnect()

    def publish(self, message):
        self.connect()
        self.client.publish(self.topic, json.dumps(message))


class MQTTSource(MQTTNode):
    mqtt_kind = 'source'

    def on_connect(self, client, userdata, flags, rc):
        client.subscribe(self.topic)

    def on_message(self, client, userdata, message):
        super().on_message(client, userdata, message)
        stream_message = {
            'topic': message.topic,
            'payload': message.payload,
            'qos': message.qos,
            'retain': message.retain
        }
        self._stream.append(stream_message)

    def stream(self, stream):
        self._stream = stream
        self.connect()
        self.client.loop_forever()

    def cancel(self):
        self.client.disconnect()


class MQTTSink(MQTTNode):
    mqtt_kind = 'sink'

    def put(self, message):
        self.publish(message)
