from minibatch import connectdb


def delete_database(url=None, dbname='test'):
    """ test support """
    db = connectdb(url=url, dbname=dbname)
    db.client.drop_database(dbname)
    return db
