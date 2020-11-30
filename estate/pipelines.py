# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import datetime
import json

import pymongo as pymongo
from itemadapter import ItemAdapter
from pymongo.errors import DuplicateKeyError

from estate.items import LianJiaSecondHandItem, LianJiaSecondHandChengjiaoItem


class EstatePipeline:
    def process_item(self, item, spider):
        return item


class MongoPipeline:

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.db[LianJiaSecondHandItem.collection].create_index("house_id", unique=True)
        self.db[LianJiaSecondHandChengjiaoItem.collection].create_index("lianjia_id", unique=True)

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        item['update_date'] = datetime.datetime.now()
        try:
            if isinstance(item, LianJiaSecondHandItem):
                self.db[LianJiaSecondHandItem.collection].update({'house_id': item.get('house_id')},
                                                                 {'$set': ItemAdapter(item).asdict()}, True)
            else:
                self.db[LianJiaSecondHandChengjiaoItem.collection].update({'lianjia_id': item.get('lianjia_id')},
                                                                          {'$set': ItemAdapter(item).asdict()}, True)
            return item
        except DuplicateKeyError:
            spider.logger.debug(' duplicate key error collection')
            return item
