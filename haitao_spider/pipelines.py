# -*- coding: utf-8 -*-

from pymongo import MongoClient
from scrapy.conf import settings


# 爬取的数据存入mongodb数据库
class HaitaoSpiderPipeline(object):
    def __init__(self):
        connection = MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        db = connection[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]

    def process_item(self, item, spider):
        url_find = {'pro_website': item['pro_website']}
        find_result = self.collection.find_one(url_find)
        if find_result:
            old_price = find_result['pro_price_new']
            if float(item['pro_price_new']) != float(old_price):
                print("***************旧数据，价格有所变动，直接删除后插入最新数据***************\n{}".format(item))
                self.collection.delete_one(url_find)
                self.collection.insert(dict(item))
        else:
            print("***************新数据，直接插入***************\n{}".format(item))
            self.collection.insert(dict(item))
        return item
