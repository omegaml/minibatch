from multiprocessing import Process, Queue
from unittest import TestCase

import sys
import time
import pathlib

from minibatch import Buffer, connectdb, logger, get_or_create_stream, close_sa
from minibatch.tests.util import delete_database
# use this for debugging subprocesses
# logger = multiprocessing.log_to_stderr()
# logger.setLevel('INFO')
from minibatch.window import CountWindow


def sleepdot(seconds=1):
    mult = 10  # ensure integers
    iterations = int(seconds * mult)
    for _ in range(0, iterations, mult):
        time.sleep(1)
        sys.stdout.write('.')
        sys.stdout.flush()


class MiniBatchTests(TestCase):
    def setUp(self):
        self.url = f'sqlite:///{pathlib.Path().resolve()}/test.db'
        delete_database(url=self.url)
        self.db = connectdb(url=self.url)
        self.processed = []

    def tearDown(self):
        close_sa()
        self.processed = None

    def sleep(self, seconds):
        sleepdot(seconds)

    def test_stream(self):
        """
        Test a stream writes to a buffer
        """
        stream = get_or_create_stream('test', url=self.url)
        stream.append({'foo': 'bar1'})
        stream.append({'foo': 'bar2'})
        count = len(list(doc for doc in self.db.query(Buffer).all()))
        self.assertEqual(count, 2)

    def test_fixed_size(self):
        """
        Test batch windows of fixed sizes work ok
        """
        from minibatch import streaming

        def consumer(q):
            logger.debug("starting consumer on {self.url}".format(**locals()))
            url = str(self.url)

            # note the stream decorator blocks the consumer and runs the decorated
            # function asynchronously upon the window criteria is satisfied
            @streaming('test', size=2, keep=True, url=self.url, queue=q)
            def myprocess(window):
                logger.debug("*** processing")
                try:
                    self.processed.append({'data': window.data or {}})
                except Exception as e:
                    print(e)
                    raise

        # start stream consumer
        q = Queue()
        stream = get_or_create_stream('test', url=self.url)
        proc = Process(target=consumer, args=(q,))
        proc.start()
        # fill stream
        for i in range(10):
            stream.append({'index': i})
        # give it some time to process
        logger.debug("waiting")
        self.sleep(10)
        q.put(True)   # stop @streaming
        proc.join()
        # expect 5 entries, each of length 2
        data = list(doc for doc in self.processed)
        count = len(data)
        self.assertEqual(count, 5)
        self.assertTrue(all(len(w) == 2 for w in data))

    def test_timed_window(self):
        """
        Test timed windows work ok
        """
        from minibatch import streaming

        def consumer(q):
            # note the stream decorator blocks the consumer and runs the decorated
            # function asynchronously upon the window criteria is satisfied

            @streaming('test', interval=1, relaxed=False, keep=True, queue=q)
            def myprocess(window):
                try:
                    self.processed.append({'data': window.data or {}})
                except Exception as e:
                    print(e)
                return window

        # start stream consumer
        q = Queue()
        stream = get_or_create_stream('test', url=self.url)
        proc = Process(target=consumer, args=(q,))
        proc.start()
        # fill stream
        for i in range(10):
            stream.append({'index': i})
            self.sleep(.5)
        # give it some time to process
        self.sleep(5)
        q.put(True)
        proc.join()
        # expect at least 5 entries (10 x .5 = 5 seconds), each of length 1-2
        data = list(doc for doc in self.processed)
        count = len(data)
        self.assertGreater(count, 5)
        self.assertTrue(all(len(w) >= 2 for w in data))

    def test_timed_window_relaxed(self):
        """
        Test relaxed timed windows work ok
        """
        from minibatch import streaming

        def consumer(q):
            # note the stream decorator blocks the consumer and runs the decorated
            # function asynchronously upon the window criteria is satisfied
            url = str(self.url)

            @streaming('test', interval=1, relaxed=True, keep=True, queue=q, url=url)
            def myprocess(window):
                try:
                    self.processed.append({'data': window.data or {}})
                except Exception as e:
                    print(e)
                return window

        # start stream consumer
        q = Queue()
        stream = get_or_create_stream('test', url=self.url)
        proc = Process(target=consumer, args=(q,))
        proc.start()
        # fill stream
        for i in range(10):
            stream.append({'index': i})
            self.sleep(.5)
        # give it some time to process
        self.sleep(5)
        q.put(True)
        proc.join()
        # expect at least 5 entries (10 x .5 = 5 seconds), each of length 1-2
        data = list(doc for doc in self.processed)
        count = len(data)
        self.assertGreater(count, 5)
        self.assertTrue(all(len(w) >= 2 for w in data))

    def _do_test_slow_emitfn(self, workers=None, expect_fail=None, timeout=None):
        """
        Test slow batch windows work properly using {workers} workers
        """
        from minibatch import streaming

        MiniBatchTests._do_test_slow_emitfn.__doc__ = MiniBatchTests._do_test_slow_emitfn.__doc__.format(
            workers=workers)

        def consumer(workers, q):
            logger.debug("starting consumer on={self.url} workers={workers}".format(**locals()))
            url = str(self.url)

            # note the stream decorator blocks the consumer and runs the decorated
            # function asynchronously upon the window criteria is satisfied
            @streaming('test', size=2, keep=True, url=self.url, max_workers=workers, queue=q)
            def myprocess(window):
                logger.debug("*** processing {}".format(window.data))
                try:
                    sleepdot(5)
                    self.processed.append({'data': window.data or {}})
                except Exception as e:
                    logger.error(e)
                    raise
                return window

        def check():
            # expect 5 entries, each of length 2
            data = list(doc for doc in self.processed)
            count = len(data)
            logger.debug("data={}".format(data))
            self.assertEqual(count, 5)
            self.assertTrue(all(len(w) == 2 for w in data))

        # start stream consumer
        # -- use just one worker, we expect to fail
        stream = get_or_create_stream('test', url=self.url)
        q = Queue()
        proc = Process(target=consumer, args=(workers, q))
        proc.start()
        # fill stream
        for i in range(10):
            stream.append({'index': i})
        # give it some time to process
        logger.debug("waiting")
        # note it takes at least 25 seconds using 1 worker (5 windows, 5 seconds)
        # so we expect to fail
        self.sleep(12)
        q.put(True)
        if expect_fail:
            with self.assertRaises(AssertionError):
                check()
        else:
            check()
        # wait for everything to terminate, avoid stream corruption in next test
        self.sleep(timeout)
        proc.join()

    def test_slow_emitfn_single_worker(self):
        self._do_test_slow_emitfn(workers=1, expect_fail=True, timeout=30)

    def test_slow_emitfn_parallel_workers(self):
        self._do_test_slow_emitfn(workers=5, expect_fail=False, timeout=12)

    def test_buffer_cleaned(self):
        stream = get_or_create_stream('test', url=self.url)
        stream.append({'foo': 'bar1'})
        stream.append({'foo': 'bar2'})

        em = CountWindow('test')
        em._run_once()
        em._run_once()

        docs = self.db.query(Buffer).all()
        self.assertEqual(len(docs), 0)
