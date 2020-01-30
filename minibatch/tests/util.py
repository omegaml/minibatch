
def delete_database():
    """ test support """
    from omegaml.mongoshim import MongoClient

    mongo_url = settings().OMEGA_MONGO_URL
    parsed_url = urlparse.urlparse(mongo_url)
    database_name = parsed_url.path[1:]
    # authenticate via admin db
    # see https://stackoverflow.com/a/20554285
    c = MongoClient(mongo_url, authSource='admin')
    c.drop_database(database_name)
s