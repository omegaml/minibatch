import os
from concurrent.futures._base import Executor, Future

from minibatch import connectdb


def delete_database(url=None, dbname='test'):
    """ test support """
    if url and url.startswith('sqlite'):
        path = url.replace("sqlite:///", "")
        if os.path.isfile(path):
            os.remove(path)
    else:
        db = connectdb(url=url, dbname=dbname)
        db.client.drop_database(dbname)


class LocalExecutor(Executor):
    def submit(self, fn):
        result = fn()
        future = Future()
        future.set_result(result)
        return future
