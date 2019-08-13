# coding:utf-8
import redis
import pymongo
import pandas as pd
import time
import requests
import json
import threading


# df = pd.read_excel(r'C:\Users\Administrator\Documents\WeChat Files\Bo_yixin\FileStorage\File\2019-07\d88_store_info.xlsx')
# df2 = pd.read_excel(r'C:\Users\Administrator\Desktop\d88_offline.dp_is_database_new.xls', usecols=[1, 3])
# df2 = df2.drop_duplicates('d88_brand_name', 'first')
# rel = pd.merge(df, df2, how='left', on='d88_brand_name')
# df['id'] = df['id'] - 3
# order = ['id', 'shop_name', 'shop_id', 'floor', 'phoneNum', 'store_type', 'dobusiness', 'thumbUrl']
# df = df[order]
# df['create_time'] = int(time.time())
# df['update_time'] = df['create_time']
# df.to_excel(r'C:\Users\Administrator\Desktop\d88_store_info.xlsx', index=False)


def fetch(url, headers, collection, i):
    response = requests.get(url, headers=headers)
    info = json.loads(response.text)
    datas = info.get('data')
    if datas:
        for data in datas:
            item = {}
            item['d88_brand_name'] = i[0]
            item['store_id'] = data.get('id')
            item['store_name'] = data.get('store_name')
            item['store_type'] = data.get('store_type')
            item['store_type_name'] = data.get('store_type_name')
            item['search_name'] = i[6]
            print(item)
            collection.insert(item)


def parse(response, collection, i):
    info = json.loads(response.text)
    datas = info.get('data')
    if datas:
        for data in datas:
            item = {}
            item['d88_brand_name'] = i[0]
            item['store_id'] = data.get('id')
            item['store_name'] = data.get('store_name')
            item['store_type'] = data.get('store_type')
            item['store_type_name'] = data.get('store_type_name')
            item['search_name'] = i[6]
            print(item)
            collection.insert(item)


def run():
    mongocli = pymongo.MongoClient('192.168.2.218', 27017)
    collection = mongocli['d88_info']['search_result2']
    df = pd.read_excel(r'C:\Users\Administrator\Desktop\1122.xls')
    df = df.loc[df['store_id'].isna()]
    df['brand_name'] = df['d88_brand_name'].str.extract(r'([\u4e00-\u9fa5]+)')
    df['brand_name'] = df['brand_name'].combine_first(df['d88_brand_name'])
    for i in df.values.tolist():
        url = 'https://offline.d88.ink/search/search_wechat?keyword={}&page=1&pagesize=10&type=0'.format(i[6])
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'}
        t = threading.Thread(target=fetch, args=(url, headers, collection, i,))
        t.start()
        t.join()
        # parse(response, collection, i)


if __name__ == '__main__':
    run()
