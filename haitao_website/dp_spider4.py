# coding:utf-8
# _*_ coding:utf-8 _*_
from fake_useragent import UserAgent
import requests
from lxml import etree
import pymongo
import re
import time
import hashlib
import pandas as pd
import redis
import random
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# 禁用安全请求警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def time_decorator(run):
    def func(*args, **kwargs):
        start_time = time.time()
        run(*args, **kwargs)
        end_time = time.time()
        print('抓取耗时%s秒' % (end_time-start_time))
    return func


class DpSpider(object):
    def __init__(self):
        self.ua = pd.read_excel('D:/fidder/ua_string.xls')['ua'].values.tolist()
        self.cookies = pd.read_excel('C:/Users/Administrator/Desktop/dp_cookies.xls')['cookie'].values.tolist()
        self.rediscli = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.df = pd.read_csv('C:/Users/Administrator/Desktop/D88.dp_shops(1).csv', usecols=[5, 7]).values.tolist()

    def get_proxies(self):
        data = self.rediscli.hgetall('useful_proxy')
        proxies = ["http://{}".format(proxy.decode()) for proxy in data]
        while True:
            proxy_1 = random.choice(proxies)
            proxy_2 = {
                "http": proxy_1,
            }
            res = requests.get("https://www.baidu.com", proxies=proxy_2)
            if res.status_code == 200:
                return proxy_1

    def parse(self, shop_id, url):
        # s.keep_alive = False
        # s.adapters.DEFAULT_RETRIES = 5
        headers = {'User-Agent': random.choice(self.ua),
                   'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                   'accept-encoding': 'gzip, deflate, br',
                   'accept-language': 'zh-CN,zh;q=0.9',
                   'cache-control': 'max-age=0',
                   'Connection': 'keep-alive',
                   'cookie': random.choice(self.cookies),
                   'Host': 'm.dianping.com',
                   'upgrade-insecure-requests': '1'}
        md5_url = hashlib.md5(url.encode('utf-8')).hexdigest()
        print(md5_url)
        print(url)
        consult = self.rediscli.sadd('dp_request_queue', md5_url)
        if consult:
            try:
                proxy = self.get_proxies()
                print(proxy)
                response = requests.get(url, headers=headers, proxies={"http": proxy}, verify=False, timeout=5,
                                        allow_redirects=False)
            except Exception as e:
                print(e)
                self.rediscli.sadd('dp_error_url', url)
                self.rediscli.srem('dp_request_queue', md5_url)
            else:
                if response.status_code != 200:
                    print(response.status_code)
                    self.rediscli.sadd('dp_error_url', url)
                    self.rediscli.srem('dp_request_queue', md5_url)
                    return
                resp = response.content.decode('utf-8')
                html = etree.HTML(resp)
                item = {}
                item['shop_id'] = shop_id
                item['area'] = html.xpath("//div[@class='shop-crumbs']/a[2]/text()")[0] if \
                    len(html.xpath("//div[@class='shop-crumbs']/a[2]/text()")) > 0 else None
                item['category'] = html.xpath("//div[@class='shop-crumbs']/a[last()]/text()")[0] if \
                    len(html.xpath("//div[@class='shop-crumbs']/a[last()]/text()")) > 0 else None
                item['address'] = re.findall(r'"address":"(.*?)"', resp)[0] if \
                    len(re.findall(r'"address":"(.*?)"', resp)) > 0 else None
                item['dobusiness'] = re.findall(r'class="businessHour">([a-zA-Z0-9\u4e00-\u9fff\s:\-,]+)</div>', resp)[0].replace(' ', '').replace('\n', '') if\
                    len(re.findall(r'class="businessHour">([a-zA-Z0-9\u4e00-\u9fff\s:\-,]+)</div>', resp)) > 0 else None
                item['phoneNum'] = re.findall(r'"phoneNum":"([0-9\-]+)"', resp)[0] if len(
                    re.findall(r'"phoneNum":"([0-9\-]+)"', resp)) > 0 else None
                item['thumbUrl'] = re.findall(r'"thumbUrl":"(.*?)"', resp)[0] if len(
                    re.findall(r'"thumbUrl":"(.*?)"', resp)) > 0 else None
                print(item)
                self.save_content_list(item)

    @staticmethod
    def save_content_list(result):
        MONGO_URL = 'localhost'
        MONGO_DB = 'haitao'
        MONGO_TABLE = 'dp_shop'
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DB]
        collection = db[MONGO_TABLE]
        collection.insert(dict(result))

    @time_decorator
    def run(self):
        for info in self.df[9000:12000]:
            time.sleep(random.uniform(0, 1))
            self.parse(info[0], info[1])


if __name__ == '__main__':
    dp_spider = DpSpider()
    dp_spider.run()


