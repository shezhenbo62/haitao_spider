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
    def __init__(self, proxies):
        self.ua = pd.read_excel('D:/fidder/ua_string.xls')['ua'].values.tolist()
        self.rediscli = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.df = pd.read_csv('C:/Users/Administrator/Desktop/D88.dp_shops(1).csv', usecols=[5, 7]).values.tolist()
        self.proxies = proxies
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36',
                   'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                   'accept-encoding': 'gzip, deflate, br',
                   'accept-language': 'zh-CN,zh;q=0.9',
                   'cache-control': 'max-age=0',
                   'Connection': 'keep-alive',
                   'cookie': '__mta=216347305.1561034903867.1561092565609.1561092710219.7; cityid=7; _lxsdk_cuid=16b6f2ea3a3c8-088b4affd26b11-e343166-1fa400-16b6f2ea3a3c8; _lxsdk=16b6f2ea3a3c8-088b4affd26b11-e343166-1fa400-16b6f2ea3a3c8; _hc.v=fb324b35-1095-d4ee-0633-029eae23f274.1560938456; logan_custom_report=; switchcityflashtoast=1; dp_pwa_v_=cede7d743c680cf24367c7011286d76f3df2fce5; source=m_browser_test_33; default_ab=shop%3AA%3A5%7Cindex%3AA%3A1%7CshopList%3AC%3A4; logan_session_token=xn15r1aqg0dagx0o0a6q; citypinyin=shenzhen; cityname=5rex5Zyz; m_flash2=1; cy=7; cye=shenzhen; pvhistory="6L+U5ZuePjo8L3N0YXRpY3Rlc3QvbG9nZXZlbnQ/bmFtZT1XaGVyZUFtSUZhaWwmaW5mbz1odG1sLSU1QiU3QiUyMmNvZGUlMjIlM0EyJTJDJTIybWVzc2FnZSUyMiUzQSUyMk5ldHdvcmslMjBsb2NhdGlvbiUyMHByb3ZpZGVyJTIwYXQlMjAlMjdodHRwcyUzQSUyRiUyRnd3dy5nb29nbGVhcGlzLmNvbSUyRiUyNyUyMCUzQSUyME5vJTIwcmVzcG9uc2UlMjByZWNlaXZlZC4lMjIlN0QlNUQmY2FsbGJhY2s9V2hlcmVBbUkxMTU2MTA5Mjc5OTM4ND46PDE1NjEwOTI3OTkxNDJdX1s="; msource=default; _lxsdk_s=16b780bfa77-258-282-2c4%7C%7C319',
                   'Host': 'm.dianping.com',
                   'upgrade-insecure-requests': '1'}

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

    def parse(self, shop_id, url, i, cookies):
        # s.keep_alive = False
        # s.adapters.DEFAULT_RETRIES = 5
        md5_url = hashlib.md5(url.encode('utf-8')).hexdigest()
        print(md5_url)
        print(url)
        consult = self.rediscli.sadd('dp_request_queue', md5_url)
        if consult:
            try:
                # proxy = self.get_proxies()
                # print(proxy)
                response = requests.get(url, headers=self.headers, proxies=self.proxies, verify=False, timeout=5,
                                        allow_redirects=False)
            except Exception as e:
                print(e)
                self.rediscli.sadd('dp_error_url', url)
                self.rediscli.srem('dp_request_queue', md5_url)
                return i
            else:
                if response.status_code == 302:
                    print(response.status_code)
                    print(i)
                    # 获取重定向链接
                    resume_url = response.headers['location']
                    print(resume_url)
                    if 'verify.meituan.com' in resume_url:
                        self.headers['cookie'] = cookies[i].decode('utf-8')
                        self.headers['User-Agent'] = random.choice(self.ua)
                        self.rediscli.sadd('dp_error_url', url)
                        self.rediscli.srem('dp_request_queue', md5_url)
                        i += 1
                        return i
                    else:
                        self.rediscli.sadd('dp_error_url', url)
                        self.rediscli.srem('dp_request_queue', md5_url)
                        return i
                time.sleep(random.uniform(0.5, 1))
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
                return i
        else:
            return i

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
        i = 0
        j = 36351
        for info in self.df[36351:40000]:
            cookies = self.rediscli.lrange('dp_cookies', 0, -1)
            print(j)
            i = self.parse(info[0], info[1], i, cookies)
            if i == len(cookies):
                i = 0
            j += 1


if __name__ == '__main__':
    # 代理服务器
    proxyHost = "http-dyn.abuyun.com"
    proxyPort = "9020"

    # 代理隧道验证信息
    proxyUser = "HGI6PG1J9C67883D"
    proxyPass = "B3FB524961981D6A"

    proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
        "host": proxyHost,
        "port": proxyPort,
        "user": proxyUser,
        "pass": proxyPass,
    }

    proxies = {
        "http": proxyMeta,
        "https": proxyMeta,
    }
    dp_spider = DpSpider(proxies)
    dp_spider.run()

