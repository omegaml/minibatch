from kafka import KafkaConsumer
from json import loads


class KafkaSource:
    """
    A kafka topic source

    Usage:
        # start consuming from Kafka
        stream = mb.stream('test')
        source = KafkaSource('kafka-topic', urls=['kafka:9092'])
        stream.attach(source)

        # stream to a python callable
        streaming('test')(lambda v: print(v))

    Args:
        topic (str): the kafka topic
        urls (list): the kafka broker urls, defaults to localhost:9092
        **configs: the keyword parameters to use on the kafka.KafkaConsumer
           defaults to dict(bootstrap_servers=urls or ['localhost:9092'],
            auto_offset_reset='earliest', enable_auto_commit=True,
            group_id='group',
            value_deserializer=lambda x: loads(x.decode('utf-8'))
    """
    def __init__(self, topic, urls=None, **configs):
        self.topic = topic
        if isinstance(urls, str):
            urls = [urls]
        self.configs = dict(
            bootstrap_servers=urls or ['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='group',
            value_deserializer=lambda x: loads(x.decode('utf-8'))
        )
        self.configs.update(configs)
        self._consumer = None

    @property
    def consumer(self):
        if self._consumer is None:
            self._consumer = KafkaConsumer(self.topic, **self.configs)
        return self._consumer

    def stream(self, stream):
        for message in self.consumer:
            stream.append(message.value)
