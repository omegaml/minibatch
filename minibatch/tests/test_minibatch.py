from multiprocessing import Process, Queue
from unittest import TestCase

import logging
import multiprocessing
import sys
import time

from minibatch import Stream, Buffer, connectdb, reset_mongoengine
from minibatch.models import Participant
from minibatch.tests.util import delete_database
from minibatch.window import CountWindow

# use this for debugging subprocesses
logLevel = logging.INFO
logging.basicConfig(level=logLevel, force=True)
logger = multiprocessing.get_logger()
multiprocessing.log_to_stderr()
logger.setLevel(logLevel)


def sleepdot(seconds=1):
    mult = 10  # ensure integers
    iterations = int(seconds * mult)
    for _ in range(0, iterations, mult):
        time.sleep(1)
        sys.stdout.write('.')
        sys.stdout.flush()


class MiniBatchTests(TestCase):
    def setUp(self):
        self.url = 'mongodb://localhost/test'
        delete_database(url=self.url)
        self.db = connectdb(url=self.url)
        self.procs = []  # list of (queue, process)

    def tearDown(self):
        reset_mongoengine()
        Participant.shutdown()

    def sleep(self, seconds):
        sleepdot(seconds)

    def test_stream(self):
        """
        Test a stream writes to a buffer
        """
        stream = Stream.get_or_create('test', url=self.url)
        stream.append({'foo': 'bar1'})
        stream.append({'foo': 'bar2'})
        count = len(list(doc for doc in Buffer.objects.all()))
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
            @streaming('test', size=2, keep=True, url=self.url, queue=q, chord='default')
            def myprocess(window):
                logger.debug("*** processing")
                try:
                    db = connectdb(url)
                    db.processed.insert_one({'data': window.data or {}})
                except Exception as e:
                    print(e)
                    raise

        # start stream consumer
        q = Queue()
        stream = Stream.get_or_create('test', url=self.url)
        proc = Process(target=consumer, args=(q,))
        proc.start()
        # fill stream
        for i in range(10):
            stream.append({'index': i})
        # give it some time to process
        logger.debug("waiting")
        self.sleep(10)
        q.put(True)  # stop @streaming
        proc.join()
        # expect 5 entries, each of length 2
        data = list(doc for doc in self.db.processed.find())
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
            url = str(self.url)

            @streaming('test', interval=1, relaxed=False, keep=True, queue=q, url=self.url)
            def myprocess(window):
                try:
                    db = connectdb(url=url)
                    db.processed.insert_one({'data': window.data or {}})
                except Exception as e:
                    print(e)
                    raise
                # return window

        # start stream consumer
        q = Queue()
        stream = Stream.get_or_create('test', url=self.url)
        proc = Process(target=consumer, args=(q,))
        proc.start()
        # fill stream
        for i in range(10):
            stream.append({'index': i})
            self.sleep(.5)
        # give it some time to process
        self.sleep(10)
        q.put(True)
        proc.join()
        # expect at least 5 entries (10 x .5 = 5 seconds), each of length 1-2
        data = list(doc for doc in self.db.processed.find())
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
                    db = connectdb(url)
                    db.processed.insert_one({'data': window.data or {}})
                except Exception as e:
                    print(e)
                return window

        # start stream consumer
        q = Queue()
        stream = Stream.get_or_create('test', url=self.url)
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
        data = list(doc for doc in self.db.processed.find())
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
            @streaming('test', size=2, keep=True, url=self.url, max_workers=workers, queue=q, chord='default')
            def myprocess(window):
                logger.debug("*** processing {}".format(window.data))
                from minibatch import connectdb
                try:
                    sleepdot(5)
                    db = connectdb(url=url)
                    db.processed.insert_one({'data': window.data or {}})
                except Exception as e:
                    logger.error(e)
                    raise
                return window

        def check():
            # expect 5 entries, each of length 2
            data = list(doc for doc in self.db.processed.find())
            count = len(data)
            logger.debug("data={}".format(data))
            self.assertEqual(count, 5)
            self.assertTrue(all(len(w) == 2 for w in data))

        # start stream consumer
        # -- use just one worker, we expect to fail
        stream = Stream.get_or_create('test', url=self.url)
        q = Queue()
        proc = Process(target=consumer, args=(workers, q))
        proc.start()
        # fill stream
        for i in range(10):
            stream.append({'index': i})
        # give it some time to process
        # note it takes at least 25 seconds using 1 worker (5 windows, 5 seconds)
        logger.debug("waiting")
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
        self._do_test_slow_emitfn(workers=5, expect_fail=False, timeout=15)

    def test_buffer_cleaned(self):
        stream = Stream.get_or_create('test', url=self.url)
        stream.append({'foo': 'bar1'})
        stream.append({'foo': 'bar2'})

        # 0.6.0: set chord explicitly to disable automated routing
        em = CountWindow('test', chord='default')
        em._run_once()
        em._run_once()

        docs = list(Buffer.objects.filter())
        self.assertEqual(0, len(docs))

    def test_buffer_housekeeping(self):
        stream = Stream.get_or_create('test', url=self.url, max_age=.5)
        stream.append({'foo': 'bar1'})
        stream.append({'foo': 'bar1'})
        stream.append({'foo': 'bar1'})
        # expect buffer contains all 3 entries
        self.assertEqual(stream.buffer().count(), 3)
        # wait for housekeeping to take effect
        time.sleep(1)
        # expect buffer is empty
        self.assertEqual(0, stream.buffer().count())
        stream.stop()

    def test_participants(self):
        # basic functions
        participant = Participant.register('test', 'producer')
        self.assertEqual(participant.stream, 'test')
        self.assertEqual(participant.role, 'producer')
        self.assertTrue(participant.active)
        participant.ACTIVE_INTERVAL = 0  # simulate inactivity
        participant.GRACE_PERIOD = 0  # simulate no grace period
        self.assertFalse(participant.active)
        participant.ACTIVE_INTERVAL = 1  # simulate short activity timeout
        participant.GRACE_PERIOD = 5  # simulate short grace period
        participant.beat()
        self.assertTrue(participant.active)
        participant.leave()
        if Participant.objects.count() > 0:
            print("participants", Participant.objects.all())
        self.assertEqual(0, Participant.objects.count(), f"participant not removed, got {Participant.objects.all()}")
        # leadership
        # -- check leader is selected automatically and correctly
        participants = []
        for i in range(10):
            participants.append(Participant.register('test', 'producer', hostname=str(i)))
        self.assertEqual(10, Participant.objects.count(), f'expected 10 participants, got {Participant.objects.all()}')
        max_elector = max(p.elector for p in Participant.objects().no_cache())
        leader = Participant.leader('test')
        self.assertIsInstance(leader, Participant)
        self.assertEqual(max_elector, leader.elector)
        # modify participant lists
        # -- remove current leader and a few more
        leader.leave()
        for p in list(Participant.for_stream('test'))[0:5]:
            p.leave()
        self.assertEqual(4, Participant.objects.count(),
                         f'expected 4 participants left, got {Participant.objects.all()}')
        leader2 = Participant.leader('test')
        self.assertNotEqual(leader.hostname, leader2.hostname)
        # -- remove all participants
        for p in Participant.for_stream('test'):
            p.leave()
        self.assertEqual(0, Participant.objects.count(), f"participant not removed, got {Participant.objects.all()}")

    def test_participants_multiple(self):
        # attempt to catch concurrency errors
        for i in range(10):
            self.test_participants()

    def test_chords_single_consumer(self):
        Participant.ACTIVE_INTERVAL = 1  # simulate short activity timeout
        stream = Stream.get_or_create('test-single', url=self.url)
        stream.append({'foo': 'bar'})
        self.assertEqual(1, len(Participant.producers(stream)))
        self.assertEqual(1, stream.buffer().count())
        self.assertEqual(1, len(stream.as_producer.chords))
        self.assertEqual(1, len(stream.as_consumer.chords))
        stream.stop()

    def test_chords_multiple_consumers(self):
        Participant.ACTIVE_INTERVAL = 1  # simulate short activity timeout
        stream = Stream.get_or_create('test-multi', url=self.url)
        workers = 5
        procs = self._consumer_pool(workers=workers, stream='test-multi', size=2, keep=True)
        assertEventually(lambda: len(stream.as_producer.chords) == workers,
                         lambda: f"expected {workers} chords, got {len(stream.as_producer.chords)}",
                         timeout=15)
        # fill stream
        for i in range(100):
            stream.append({'index': i})
            # print("all chords", stream.as_producer._all_chords)
        assertEventually(lambda: len(set(w.chord for w in stream.buffer())) == workers,
                         lambda: f"expected 5 chords, got {len(set(w.chord for w in stream.buffer()))}",
                         timeout=15)
        self.assertEqual(workers, len(Participant.for_stream(stream, role='consumer')))
        stream.stop()
        self._close_pool(procs)
        print("done")

    def _consumer_pool(self, workers=1, stream='test', **kwargs):
        procs = []
        for i in range(workers):
            q, consumer = self._pooled_consumer()
            proc = Process(target=consumer, args=(q, stream, self.url, kwargs))
            proc.start()
            procs.append((q, proc))
        return procs

    def _close_pool(self, procs):
        for q, proc in procs:
            q.put(True)
            proc.join()
        logger.info("pool closed")

    def _pooled_consumer(self):
        def consumer(q, stream, url, kwargs):
            from minibatch import streaming
            from minibatch import connectdb

            logger.info(f"starting consumer on={url}")

            # note the stream decorator blocks the consumer and runs the decorated
            # function asynchronously upon the window criteria is satisfied
            @streaming(stream, queue=q, url=url, **kwargs)
            def myprocess(window):
                logger.info("*** processing {}".format(window.data))
                try:
                    sleepdot(5)
                    db = connectdb(url=url)
                    db.processed.insert_one({'data': window.data or {}})
                except Exception as e:
                    logger.error(e)
                    raise
                return window

        q = Queue()
        return q, consumer


def assertEventually(condition, msg=None, timeout=10, interval=1):
    for _ in range(0, timeout, interval):
        if condition():
            return
        time.sleep(interval)
    msg = msg() if callable(msg) else (msg or "condition not met")
    raise AssertionError(msg)
