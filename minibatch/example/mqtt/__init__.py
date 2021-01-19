from multiprocessing import Process

from datetime import datetime
from time import sleep

from minibatch import connectdb, streaming, stream
from minibatch.contrib.mqtt import MQTTSource, MQTTSink
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
    mqtt_broker = 'mqtt://rabbitmq:rabbitmq@localhost'
    topic = 'TEST/MESSAGE'
    source = MQTTSource(mqtt_broker, topic)
    producer = MQTTSink(mqtt_broker, topic)
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
    db = connectdb()
    docs = list(doc for doc in db.processed.find())
    print("processed items:", len(docs))
    print(docs)
    emitp.terminate()
    source.disconnect()
    producer.disconnect()
