# -*- coding: utf-8 -*-

from pymongo import MongoClient
from scrapy.conf import settings


# 爬取的数据存入mongodb数据库且根据url和activity去重
class KaoLaPipeline(object):
    def __init__(self):
        connection = MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        db = connection[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]

    def process_item(self, item, spider):
        url_find = {'goods_url': item['goods_url']}
        find_result = self.collection.find_one(url_find)
        if find_result:
            old_activity = find_result['activity']
            if item['activity'] != old_activity:
                print("***************旧数据，但是活动更新***************\n{}".format(item))
                self.collection.update(url_find, {'$set': {"activity": item['activity'], "create_time": item['create_time']}})
        else:
            print("***************此条数据为更新数据***************\n{}".format(item))
            self.collection.insert(dict(item))
        return item


class JdElectPipeline(object):
    def __init__(self):
        connection = MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        db = connection[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]

    def process_item(self, item, spider):
        url_find = {'goods_url': item['goods_url']}
        find_result = self.collection.find_one(url_find)
        if find_result:
            old_price = find_result['price']
            if float(item['price']) != float(old_price):
                print("***************旧数据，价格有所变动，直接删除后插入最新数据***************\n{}".format(item))
                self.collection.delete_one(url_find)
                self.collection.insert(dict(item))
        else:
            print("***************新数据，直接插入***************\n{}".format(item))
            self.collection.insert(dict(item))
        return item
