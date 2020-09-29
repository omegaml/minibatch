from collections import defaultdict
from contextlib import contextmanager
from unittest import TestCase

from unittest.mock import MagicMock

from minibatch import connectdb, stream
from minibatch.contrib.celery import CeleryEventSource
from minibatch.tests.util import delete_database


class CeleryEventSourceTests(TestCase):
    def setUp(self):
        self.url = 'mongodb://localhost/test'
        delete_database(url=self.url)
        self.db = connectdb(url=self.url)

    def test_source(self):
        celeryapp = DummyCeleryApp()
        source = CeleryEventSource(celeryapp)
        s = stream('test', url=self.url)
        # mock stream append because sut is CeleryEventSource, not append
        s.append = MagicMock()
        # mock event source
        event = {
            'name': 'test',
            'uuid': '12345',
            'state': 'SUCCESS',
            'runtime': 1.0,
        }
        celeryapp.source = source
        celeryapp.dummy_events = [event]
        s.attach(source)
        source.stream(s)
        s.append.assert_called()
        s.stop()


class attrdict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self


class DummyCeleryApp:
    def __init__(self):
        self._tasks = defaultdict(dict)

    @contextmanager
    def connection(self):
        yield self

    @property
    def events(self):
        return self

    def Receiver(self, *args, **kwargs):
        return self

    def State(self):
        return self

    def event(self, state):
        self._tasks[state['uuid']].update(state)
        v = attrdict(self._tasks[state['uuid']])
        v.info = lambda: {k: v for k, v in v.__dict__.items() if k != 'info'}
        self._tasks[state['uuid']] = v
        return self

    @property
    def tasks(self):
        return self._tasks

    def capture(self, *args, **kwargs):
        for event in self.dummy_events:
            for eventkey, handler in self.source.handlers.items():
                handler(event)
