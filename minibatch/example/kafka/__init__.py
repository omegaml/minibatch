from minibatch.contrib.kafka import KafkaSink, KafkaSource


def producer(url):
    sink = KafkaSink('test', urls=url)
    sink.put(dict(foo='bar'))

def consumer(url):
    source = KafkaSource('test-result', urls=url)
    for data in source.consumer:
        print(data)
