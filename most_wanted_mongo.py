import pymongo
from bson import ObjectId

import constants


class MWMongo(object):

    collection_name = 'most_wanted'

    def __init__(self, host, port, db_name):
        self.host = host
        self.port = port
        self.db_name = db_name

    def connect(self):
        self.client = pymongo.MongoClient(self.host, self.port)
        self.db = self.client[self.db_name]
        self.coll = self.db[self.collection_name]
        return self

    def open_spider(self, open_spider):
        self.connect()

    def close(self):
        self.client.close()

    def close_spider(self, spider):
        self.close()

    def process_item(self, item, spider):
        self.coll.insert_one(dict(item))

    def get_all(self, all_keys=False):
        if not all_keys:
            projection = {
                constants.crime_type: 1,
                constants.crime_location: 1,
                constants.suspect_name: 1,
                constants.summary: 1,
            }
        return list(self.coll.find({}, projection))

    def get_all_id_str(self, all_keys=False):
        items = []
        for item in self.get_all(all_keys):
            item['id'] = str(item.pop('_id'))
            items.append(item)
        return items

    def get_by_object_id(self, object_id):
        item = self.coll.find_one({'_id': ObjectId(object_id)})
        if item:
            item['id'] = str(item.pop('_id'))
            return item