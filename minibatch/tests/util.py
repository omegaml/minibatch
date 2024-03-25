from concurrent.futures._base import Executor, Future

from minibatch import connectdb


def delete_database(url=None, dbname='test'):
    """ test support """
    db = connectdb(url=url, dbname=dbname)
    db.client.drop_database(dbname)
    return db


class LocalExecutor(Executor):
    def __init__(self, *args, **kwargs):
        pass

    def submit(self, fn, /, *args, **kwargs):
        result = fn()
        future = Future()
        future.set_result(result)
        return future
