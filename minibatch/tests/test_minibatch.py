from multiprocessing import Process
from time import sleep
from unittest import TestCase

from minibatch import Stream, Buffer, setup
from minibatch.tests.util import delete_database


class MiniBatchTests(TestCase):
    def setUp(self):
        self.url = 'mongodb://localhost/test'
        delete_database(url=self.url)
        self.db = setup(url=self.url)

    def test_stream(self):
        """
        Test a stream writes to a buffer
        """
        stream = Stream.get_or_create('test')
        stream.append({'foo': 'bar1'})
        stream.append({'foo': 'bar2'})
        count = len(list(doc for doc in Buffer.objects.all()))
        self.assertEqual(count, 2)

    def test_fixed_size(self):
        """
        Test batch windows of fixed sizes work ok
        """
        from minibatch import streaming

        def consumer():
            # note the stream decorator blocks the consumer and runs the decorated
            # function asynchronously upon the window criteria is satisfied
            @streaming('test', size=2, keep=True)
            def myprocess(window):
                try:
                    db = setup(self.url)
                    db.processed.insert_one({'data': window.data or {}})
                except Exception as e:
                    print(e)
                return window
        # start stream consumer
        proc = Process(target=consumer)
        proc.start()
        # fill stream
        stream = Stream.get_or_create('test')
        for i in range(10):
            stream.append({'index': i})
        # give it some time to process
        sleep(5)
        proc.terminate()
        # expect 5 entries, each of length 2
        data = list(doc for doc in self.db.processed.find())
        count = len(data)
        self.assertEqual(count, 5)
        self.assertTrue(all(len(w) == 2 for w in data))

    def test_timed_window(self):
        """
        Test batch windows of fixed sizes work ok
        """
        from minibatch import streaming

        def consumer():
            # note the stream decorator blocks the consumer and runs the decorated
            # function asynchronously upon the window criteria is satisfied
            @streaming('test', interval=1, keep=True)
            def myprocess(window):
                try:
                    db = setup(url=self.url)
                    db.processed.insert_one({'data': window.data or {}})
                except Exception as e:
                    print(e)
                return window
        # start stream consumer
        proc = Process(target=consumer)
        proc.start()
        # fill stream
        stream = Stream.get_or_create('test')
        for i in range(10):
            stream.append({'index': i})
            sleep(.5)
        # give it some time to process
        sleep(5)
        proc.terminate()
        # expect at least 5 entries (10 x .5 = 5 seconds), each of length 1-2
        data = list(doc for doc in self.db.processed.find())
        count = len(data)
        self.assertGreater(count, 5)
        self.assertTrue(all(len(w) >= 2 for w in data))

    def test_timed_window_relaxed(self):
        """
        Test batch windows of fixed sizes work ok
        """
        from minibatch import streaming

        def consumer():
            # note the stream decorator blocks the consumer and runs the decorated
            # function asynchronously upon the window criteria is satisfied
            @streaming('test', interval=1, relaxed=True, keep=True)
            def myprocess(window):
                try:
                    db = setup(self.url)
                    db.processed.insert_one({'data': window.data or {}})
                except Exception as e:
                    print(e)
                return window
        # start stream consumer
        proc = Process(target=consumer)
        proc.start()
        # fill stream
        stream = Stream.get_or_create('test')
        for i in range(10):
            stream.append({'index': i})
            sleep(.5)
        # give it some time to process
        sleep(5)
        proc.terminate()
        # expect at least 5 entries (10 x .5 = 5 seconds), each of length 1-2
        data = list(doc for doc in self.db.processed.find())
        count = len(data)
        self.assertGreater(count, 5)
        self.assertTrue(all(len(w) >= 2 for w in data))

