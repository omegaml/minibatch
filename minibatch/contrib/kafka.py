from json import loads, dumps

from kafka import KafkaConsumer, KafkaProducer


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
        self._cancel = False

    @property
    def consumer(self):
        if self._consumer is None:
            self._consumer = KafkaConsumer(self.topic, **self.configs)
        return self._consumer

    def stream(self, stream):
        self._cancel = False
        for message in self.consumer:
            if self._cancel:
                break
            stream.append(message.value)

    def cancel(self):
        self._cancel = True


class KafkaSink:
    """
    A Kafka topic sink
    """

    def __init__(self, topic, urls=None, expand=True, **configs):
        """

        Args:
            topic:
            urls:
            expand: if True will send each input message seperately, if False
                will send all input messages as provided.
            **configs:
        """
        self.topic = topic
        if isinstance(urls, str):
            urls = [urls]
        self.configs = dict(
            bootstrap_servers=urls or ['localhost:9092'],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )
        self.configs.update(configs)
        self._producer = None
        self.expand = expand

    @property
    def producer(self):
        if self._producer is None:
            self._producer = KafkaProducer(**self.configs)
        return self._producer

    def put(self, message, topic=None):
        topic = topic or self.topic
        if self.expand:
            if not isinstance(message, (tuple, list)):
                message = [message]
            result = [self.producer.send(topic, value=m) for m in message]
        else:
            result = self.producer.send(topic, value=message)
        return result
