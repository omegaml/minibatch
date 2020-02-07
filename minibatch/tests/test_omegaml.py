from unittest import TestCase

try:
    from omegaml import Omega
    from time import sleep

    from minibatch import stream, connectdb
    from minibatch.contrib.omegaml import DatasetSource, DatasetSink
    from minibatch.tests.util import LocalExecutor, delete_database
    from minibatch.window import CountWindow


    class OmegamlTests(TestCase):
        def setUp(self):
            self.url = 'mongodb://localhost/test'
            delete_database(url=self.url)
            self.om = Omega(mongo_url=self.url)
            self.db = connectdb(self.url)

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
                db.processed.insert(window.data)

            om.datasets.put({'foo': 'bar'}, 'stream-test')
            sleep(1)

            em = CountWindow('test', emitfn=emit)
            em.run(blocking=False)
            sleep(1)
            s.stop()

            docs = list(db.processed.find())
            self.assertEqual(len(docs), 1)

        def test_sink(self):
            om = self.om
            db = self.db
            url = str(self.url)

            source = DatasetSource(om, 'stream-test')
            sink = DatasetSink(om, 'stream-sink')
            s = stream('test', url=url)
            s.attach(source)

            def emit(window):
                # this runs in a sep thread, so reconnect db
                db = connectdb(url)
                db.processed.insert(window.data)

            om.datasets.put({'foo': 'bar'}, 'stream-test')
            sleep(1)

            em = CountWindow('test', emitfn=emit, forwardfn=sink.put)
            em.run(blocking=False)
            sleep(1)
            s.stop()

            docs = list(db.processed.find())
            docs = list(om.datasets.collection('stream-sink').find())
            self.assertEqual(len(docs), 1)
except:
    print("WARNING could not load omegaml dependencies => omegaml dataset source/sink are not supported")


