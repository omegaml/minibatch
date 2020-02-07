from mongoengine import disconnect

from minibatch import connectdb


def clean():
    db = connectdb()
    db.drop_collection('buffer')
    db.drop_collection('stream')
    db.drop_collection('window')
    db.drop_collection('processed')
    disconnect('minibatch')
