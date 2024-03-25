import os


class ProcessLocal(dict):
    def __init__(self, *args, cache=None, **kwargs):
        self._pid = os.getpid()
        self._cache = cache
        super().__init__(*args, **kwargs)

    def _check_pid(self):
        if self._pid != os.getpid():
            self.clear()
            self._pid = os.getpid()

    def __getitem__(self, k):
        self._check_pid()
        return (self._cache or super()).__getitem__(k)

    def __setitem__(self, key, value):
        self._check_pid()
        super().__setitem__(key, value)

    def keys(self):
        self._check_pid()
        return (self._cache or super()).keys()

    def clear(self):
        self._cache.clear() if self._cache else None
        return super().clear()

    def __contains__(self, item):
        self._check_pid()
        return (self._cache or super()).__contains__(item)


class resilient:
    # make a logger that doesn't crash on exceptions
    # due to the stream being closed on exit
    def __init__(self, obj):
        self.obj = obj

    def __getattr__(self, attr):
        return resilient(getattr(self.obj, attr))

    def __call__(self, *args, **kwargs):
        try:
            return self.obj(*args, **kwargs)
        except Exception as e:
            pass
