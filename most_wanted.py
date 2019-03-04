import pymongo

class MostWantedItem(object):

    collection_name = 'most_wanted'

    def __init__(self, host, port, db_name):
        self.host = host
        self.port = port
        self.db_name = db_name

    def connect(self):
        self.client = pymongo.MongoClient(self.host, self.port)
        self.db = self.client[self.db_name]
        self.coll = self.db[self.collection_name]

    def open_spider(self, open_spider):
        self.connect()

    def close(self):
        self.client.close()

    def close_spider(self, spider):
        self.close()

    def process_item(self, item):
        self.coll.insert_one(dict(item))

    def get_all(self):
        return self.coll.find()

    def get_by_name(self, name):
        self.coll.find({'suspect_name': name})
