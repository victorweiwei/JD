# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging
from pymongo import MongoClient

class JdPipeline(object):
    def process_item(self, item, spider):
        return item
class MongoDBPipeline(object):
    def __init__(self):

        conn = MongoClient('127.0.0.1', 27017)
        self.db = conn.jd

    def process_item(self, item, spider):
        for data in item:
            if not data:
                print('data error')
            else:
                try:
                    item_name = item['item_name']
                    self.db[item_name].insert(dict(item))
                    logging.debug("add {}".format(item['item_name']))
                except Exception as err:
                    print(err)
            return item