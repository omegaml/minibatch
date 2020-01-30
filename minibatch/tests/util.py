from minibatch import setup


def delete_database(url=None, dbname='test'):
    """ test support """
    db = setup(url=url, dbname=dbname)
    db.client.drop_database(dbname)
    return db