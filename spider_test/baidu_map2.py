# coding:utf-8
import requests
import re
from fake_useragent import UserAgent
import pandas as pd
from bson import ObjectId
import time
import random
import pymongo
import redis
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# 禁用安全请求警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def get_proxies():
    rediscli = redis.StrictRedis(host='127.0.0.1', port=6379, db=0, password='2012Dibaba')
    data = rediscli.hgetall('useful_proxy')
    proxies = ["http://{}".format(proxy.decode()) for proxy in data]
    while True:
        proxy_1 = random.choice(proxies)
        proxy_2 = {
            "http": proxy_1,
        }
        res = requests.get("https://www.baidu.com", proxies=proxy_2)
        if res.status_code == 200:
            return proxy_1


def read_brand():
    df = pd.read_csv('C:/Users/Administrator/Desktop/map_position.csv')
    map_positions = df.values.tolist()
    return map_positions


def fetch(url):
    ua = UserAgent()
    headers = {'Host': 'restapi.amap.com',
               'User-Agent': ua.random,
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Accept-Encoding': 'gzip, deflate, br',
               'Connection': 'keep-alive',
               'Cookie': 'guid=9a54-a400-8758-ac33; UM_distinctid=168739223549c-08d9fdd5b470fd-37664109-1fa400-1687392235577; cna=r5cTFAVzS0gCAXFo2mWr3FNi; isg=BLa23EnX9uXM5IIYtEwnX5yGB-x4f_jNKtIL-yCfjRk0Y1b9iGUyIQQae3-qS_Ip; l=bBL_t0iVvch7pDjZBOCN5uI8U0bOAIRAguPRwd4yi_5Il6L1bJbOl3eJ0Fp6Vj5R_MLB4Avqziw9-etk9; key=8325164e247e15eea68b59e89200988b',
               'Upgrade-Insecure-Requests': '1'}
    try:
        proxy = get_proxies()
        print(proxy)
        response = requests.get(url, headers=headers, proxies={"http": proxy}, verify=False, timeout=5)
    except Exception as e:
        print(e)
        time.sleep(1)
        fetch(url)
    else:
        return response.text


def get_content(resp, longitude, latitude):
    resp = str(resp)
    item = {}
    item['_id'] = ObjectId()
    item['city'] = re.findall(r'"city":"([\w\u4e00-\u9fff]+)"', resp)[0] if len(
        re.findall(r'"city":"([\w\u4e00-\u9fff]+)"', resp)) > 0 else None
    item['address'] = re.findall(r'"formatted_address":"([\w\u4e00-\u9fff]+)"', resp)[0] if len(
        re.findall(r'"formatted_address":"([\w\u4e00-\u9fff]+)"', resp)) > 0 else None
    item['longitude'] = longitude
    item['latitude'] = latitude
    print(item)
    save_mongodb(item)


def save_mongodb(item):
    MONGO_URL = 'localhost'
    MONGO_DB = 'd88_info'
    MONGO_TABLE = 'map_position'
    client = pymongo.MongoClient(MONGO_URL)
    db = client[MONGO_DB]
    collection = db[MONGO_TABLE]
    collection.insert(item)


def run():
    map_positions = read_brand()
    for position in map_positions[550000:800000]:
        print(position)
        url = 'https://restapi.amap.com/v3/geocode/regeo?key=8325164e247e15eea68b59e89200988b&location={},{}'.format(
            float(format(position[0], '.6f')), float(format(position[1], '.6f')))
        resp = fetch(url)
        time.sleep(float(format(random.uniform(0, 1), '.2f')))
        get_content(resp, float(format(position[0], '.6f')), float(format(position[1], '.6f')))


if __name__ == '__main__':
    run()
