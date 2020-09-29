class CeleryEventSource:
    """ A CeleryEventSource

    This implements a Celery event listener that forwards task
    information to a minibatch stream

    Usage:
        # start consuming from celery
        celeryapp = celery.current_app #
        stream = mb.stream('test')
        source = CeleryEventSource(celeryapp)

        # stream consumer
        streaming('test')(lambda v: print(v))

        The information forwarded is the dict returned by the
        CeleryEventSource.task_info method:

        {
          'task_name': task.name,   # the task's anme
          'task_id': task.uuid,     # the task's uuid
          'task_info': task.info(), # the task info (kwargs, args, return value)
          'task_state': task.state, # state
          'task_runtime': task.runtime, # total runtime
        }

    Args:
        celeryapp (Celery.app): the celery application
        events (list): optional, list of events to process

    See Also
        https://docs.celeryproject.org/en/latest/userguide/monitoring.html#real-time-processing
    """
    default_events = ('task-succeeded', 'task-failed')

    def __init__(self, celeryapp, events=None):
        self.celeryapp = celeryapp
        self._stream = None
        self._state = None
        self._events = events or self.default_events
        self._recv = None

    @property
    def handlers(self):
        state = self.state
        handlers = {
            '*': state.event
        }
        for ev in self._events:
            handlers[ev] = self._append
        return handlers

    def stream(self, stream):
        # adopted from https://docs.celeryproject.org/en/stable/userguide/monitoring.html#real-time-processing
        app = self.celeryapp
        self._stream = stream
        with app.connection() as connection:
            recv = self._recv = app.events.Receiver(connection, handlers=self.handlers)
            recv.capture(limit=None, timeout=None, wakeup=True)

    @property
    def state(self):
        if self._state is None:
            self._state = self.celeryapp.events.State()
        return self._state

    @property
    def recv(self):
        return self._recv

    def task_info(self, task):
        return {
            'task_name': task.name,
            'task_id': task.uuid,
            'task_info': task.info(),
            'task_state': task.state,
            'task_runtime': task.runtime,
        }

    def _append(self, event):
        state = self.state
        # process latest event
        state.event(event)
        # get task info
        task = state.tasks.get(event['uuid'])
        # append to stream
        self._stream.append(self.task_info(task))

    def cancel(self):
        recv = self.recv
        if recv is not None:
            recv.should_stop = True
