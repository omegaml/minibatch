from unittest import TestCase

try:
    from omegaml import Omega
    from time import sleep

    from minibatch import stream, connectdb
    from minibatch.contrib.omegaml import DatasetSource, DatasetSink
    from minibatch.tests.util import delete_database, LocalExecutor
    from minibatch.window import CountWindow


    class OmegamlTests(TestCase):  # noqa: E303
        def setUp(self):
            self.url = 'mongodb://localhost/test'
            delete_database(url=self.url)
            self.om = Omega(mongo_url=self.url)
            self.db = connectdb(self.url)

        def tearDown(self):
            pass

        def test_source(self):
            om = self.om
            db = self.db
            url = str(self.url)

            source = DatasetSource(om, 'stream-test')
            s = stream('test', url=url)
            s.attach(source)

            def emit(window):
                # this runs in a sep thread, so reconnect db
                db = connectdb(url)
                db.processed.insert_many(window.data)

            om.datasets.put({'foo': 'bar'}, 'stream-test')
            sleep(2)

            em = CountWindow('test', emitfn=emit, executor=LocalExecutor())
            em.run(blocking=False)
            sleep(1)
            s.stop()

            docs = list(db.processed.find())
            self.assertEqual(len(docs), 1)

        def test_sink(self):
            om = self.om
            db = self.db  # noqa
            url = str(self.url)

            source = DatasetSource(om, 'stream-test')
            sink = DatasetSink(om, 'stream-sink')
            s = stream('test', url=url)
            s.attach(source)

            def emit(window):
                # this runs in a sep thread, so reconnect db
                db = connectdb(url)
                db.processed.insert_many(window.data)

            om.datasets.put({'foo': 'bar'}, 'stream-test')
            sleep(2)

            em = CountWindow('test', emitfn=emit, forwardfn=sink.put, executor=LocalExecutor())
            # run emitter until the message has arrived in sink
            # -- sleep() is not sufficiently stable depending on system load
            em.should_stop = lambda *args, **kwargs: om.datasets.collection('stream-sink').count_documents({}) > 0
            em.run(blocking=True)
            s.stop()

            docs = list(db.processed.find())
            docs = list(om.datasets.collection('stream-sink').find())
            self.assertEqual(len(docs), 1)


except Exception as e:  # noqa
    print("WARNING could not load omegaml dependencies => omegaml dataset source/sink are not supported")
