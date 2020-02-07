from minibatch.contrib.mongodb import MongoSource, MongoSink


class DatasetSource:
    """
    A omegaml dataset source

    Usage:
        # start consuming from dataset
        stream = mb.stream('test')
        source = DatasetSource(om, 'mydataset')
        stream.attach(source)

        # stream to a python callable
        streaming('test')(lambda v: print(v))

    Args:
        om (Omega): the omega instance
        dataset (str): the dataset name
        **source_kwargs: kwargs to the MongoSource

    Notes:

        * om.datasets is used as the interface to the omegaml analytics store,
          backed by MongoDB. Thus the actual source is MongoSource

        * if the dataset does not exist it will be created as a MongoDB
          native collection
    """

    def __init__(self, om, dataset, **source_kwargs):
        self.om = om
        self._dataset_name = dataset
        self._source = None
        self._source_kwargs = source_kwargs
        # ensure the source dataset exists
        assert self.source is not None

    @property
    def source(self):
        om = self.om
        if self._source is None:
            coll = om.datasets.collection(self._dataset_name)
            if om.datasets.metadata(self._dataset_name) is None:
                # create a collection-linked dataset if it does not exist yet
                om.datasets.put(coll, self._dataset_name)
            self._source = MongoSource(coll, **self._source_kwargs)
        return self._source

    def stream(self, stream):
        self.source.stream(stream)

    def cancel(self):
        self.source.cancel()


class DatasetSink:
    """
    A omegaml dataset sink

    Usage:
        sink = DatasetSink(om, 'mydataset')
        @streaming('stream', sink=ink)
        def process(window):
            ...

    Args:
        om (Omega): the Omega instance
        dataset (str): the name of the dataset
        **sink_kwargs (dict): the kwargs to the MongoSink

        Notes:

        * om.datasets is used as the interface to the omegaml analytics store,
          backed by MongoDB. Thus the actual sink is MongoSink

        * if the dataset does not exist it will be created as a MongoDB native
          collection
    """

    def __init__(self, om, dataset, **sink_kwargs):
        self.om = om
        self._dataset_name = dataset
        self._sink = None
        self._sink_kwargs = sink_kwargs

    @property
    def sink(self):
        om = self.om
        if self._sink is None:
            coll = om.datasets.collection(self._dataset_name)
            if om.datasets.metadata(self._dataset_name) is None:
                # create a collection-linked dataset if it does not exist yet
                om.datasets.put(coll, self._dataset_name)
            self._sink = MongoSink(coll)
        return self._sink

    def put(self, messages):
        self.sink.put(messages)
