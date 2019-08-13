# _*_ coding:utf-8 _*_
from fake_useragent import UserAgent
import requests
from multiprocessing import Pool
from lxml import etree
import re
import time
import pymongo
import datetime
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


class TmSpider(object):
    def __init__(self):
        # self.start_url = 'https://list.tmall.com/search_product.htm?q={}&type=p&vmarket=&spm=875.7931836%2FB.a2227oh.d100&from=mallfp..pc_1_searchbutton'
        self.start_url = 'https://list.tmall.com/search_product.htm?q={}&type=p&vmarket=&spm=875.7931836%2FB.a2227oh.d100&from=mallfp..pc_1_searchbutton'
        self.headers = {'User-Agent': ua.random,
                        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                        'accept-encoding': 'gzip, deflate, br',
                        'accept-language': 'zh-CN,zh;q=0.9',
                        'cache-control': 'max-age=0',
                        'cookie': 'cna=r5cTFAVzS0gCAXFo2mWr3FNi; hng=CN%7Czh-CN%7CCNY%7C156; lid=%E5%A5%BD%E7%9A%84%E4%BA%8B%E6%83%85lv; _med=dw:1920&dh:1080&pw:1920&ph:1080&ist:0; cq=ccp%3D1; _m_h5_tk=8c803d54cc721ffc2a126471ab0ad195_1559022799231; _m_h5_tk_enc=05124cda76e705c50393f33663b5596e; t=9171820ee3a05280f5797e863b13cc3d; tracknick=%5Cu597D%5Cu7684%5Cu4E8B%5Cu60C5lv; _tb_token_=e5ee55b8e335; cookie2=169eae9f9ff409f6e3604f28f479b50e; tt=tmall-main; res=scroll%3A1903*5754-client%3A1903*937-offset%3A1903*5754-screen%3A1920*1080; pnm_cku822=098%23E1hvL9vUvbpvUvCkvvvvvjiPRLchAjlnR2dpgjrCPmPyQjtPPFFh0jEjnL5OAjrWRphvCvvvvvmEvpvVmvvC9ja2uphvmvvv9bx2XnlUKphv8vvvvvCvpvvvvvm2phCv28OvvUnvphvpgvvv96CvpCCvvvm2phCvhhvEvpCW2ECrvvw0zjc6AEu4X9nr1WBlHdUf8%2BBlYPeAdcHVafmxfXkfjomxfwLyd3WDN%2BLhafmAdcHvaNLtD40OaAuQD7zhV8tKjrcnI4mAdphCvvOvCvvvphvPvpvhvv2MMTwCvvpvvUmm; l=bBSKZQvcvo9cSDNSXOCwIuI8U0b9KIRAguPRwd4yi_5BT6L_-4QOlH5FAFp6Vj5R6PLB4s6vWjp9-etki; isg=BF9fZvwbf_jdaHyGSHNt6nfr7rMpbLG6MQlgtfGsvo5VgH8C-ZN5t9lSRlBbGIve',
                        'referer': 'https://www.tmall.com/',
                        'upgrade-insecure-requests': '1'}

    def get_proxies(self):
        rediscli = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)
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

    def parse(self, url):
        # s.keep_alive = False
        # s.adapters.DEFAULT_RETRIES = 5
        try:
            proxy = self.get_proxies()
            print(proxy)
            response = requests.get(url, headers=self.headers, proxies={"http": proxy}, verify=False, timeout=5)
        except Exception as e:
            print(e)
            time.sleep(1)
            self.parse(url)
        else:
            try:
                return response.content.decode('gbk')
            except Exception as f:
                print(f)
                return None

    @staticmethod
    def get_content_info(response, brand, category):
        html = etree.HTML(response)
        div_list = html.xpath("//div[@class='product item-1111 ']")
        content_list = []
        for div in div_list:
            item = {}
            item['category'] = category
            # item['brand_name'] = brand
            goods_url = div.xpath(".//p[@class='productTitle']/a/@href")
            if len(goods_url) == 0:
                goods_url = div.xpath(".//div[@class='productImg-wrap']/a/@href")
            item['pro_website'] = 'https:' + goods_url[0]
            item['phone_goods_url'] = 'https://detail.m.tmall.com/item.' + goods_url[0].split('.')[3]
            item['pro_price_new'] = div.xpath(".//p[@class='productPrice']/em/text()")[0] if len(div.xpath(".//p[@class='productPrice']/em/text()"))>0 else None
            item['shop_name'] = div.xpath(".//p[@class='productStatus']/span[3]/@data-nick")[0] if len(div.xpath(".//p[@class='productStatus']/span[3]/@data-nick"))>0 else None
            if item['shop_name'] is None:
                data = div.xpath(".//a[@class='productShop-name']")[0]
                item['shop_name'] = data.xpath("string(.)").strip()
            item['sale_count'] = div.xpath(".//p[@class='productStatus']/span[1]/em/text()")[0] if len(div.xpath(".//p[@class='productStatus']/span[1]/descendant-or-self::*/text()"))>0 else None
            content_list.append(item)
        next_url = html.xpath("//a[@class='ui-page-s-next']/@href")[0] if len(html.xpath("//a[@class='ui-page-s-next']/@href"))>0 else None
        if next_url is not None:
            next_url = 'https://list.tmall.com/search_product.htm' + next_url
            # print(next_url)
        return content_list, next_url

    def get_detail_info(self, detail_rsp, content, info_list):
        # print(detail_rsp)
        html = etree.HTML(detail_rsp)
        content['brand_name'] = re.findall(r'{"品牌":"(.*?)"}', detail_rsp)[0].strip() if len(re.findall(r'{"品牌":"(.*?)"}', detail_rsp))>0 else None
        content['pro_title'] = html.xpath("//div[@class='main cell']/text()")[0].strip() if len(html.xpath("//div[@class='main cell']/text()"))>0 else None
        content['pro_pic'] = "https:" + re.findall(r'"images":\["(.*?)",', detail_rsp)[0] if len(re.findall(r'"images":\["(.*?)",', detail_rsp))>0 else None
        content['activity'] = re.findall(r'"content":\["(.*?)"\],', detail_rsp)[0] if len(re.findall(r'"content":\["(.*?)"\],', detail_rsp))>0 else None
        content['comment_count'] = re.findall(r'"totalCount":(\d+),', detail_rsp)[0] if len(re.findall(r'"totalCount":(\d+),', detail_rsp))>0 else None
        content['pro_price_old'] = html.xpath("//div[@class='module-price']//span/text()")[0] if len(html.xpath("//div[@class='module-price']//span/text()"))>0 else None
        content['create_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.save_content_list(content)
        return info_list

    @staticmethod
    def save_content_list(result):
        MONGO_URL = 'localhost'
        MONGO_DB = 'haitao'
        MONGO_TABLE = 'tianmao'
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DB]
        collection = db[MONGO_TABLE]
        url_find = {'pro_website': result['pro_website']}
        if collection.find_one(url_find):
            old_price = collection.find_one(url_find)['pro_price_new']
            if float(result['pro_price_new']) != float(old_price):
                print("***************旧数据，价格有所变动，直接删除后插入最新数据***************\n{}".format(result))
                collection.delete_one(url_find)
                collection.insert(dict(result))
        else:
            print("***************新数据，直接插入***************\n{}".format(result))
            collection.insert(dict(result))

    @time_decorator
    def run(self):
        p = Pool(processes=10)
        client = pymongo.MongoClient('localhost', 27017)
        db = client['brand']
        D88_brand = db['D88_brand']
        data = pd.DataFrame(list(D88_brand.find()))
        data = data.drop(columns=['_id'])
        data = data.values.tolist()
        for info in data:
            brand = info[0]
            category = info[1]
            next_url = self.start_url.format(brand)
            info_list = []
            while next_url:
                response = self.parse(next_url)
                if response is not None:
                    content_list, next_url = self.get_content_info(response, brand, category)
                    print('下一页', next_url)
                    for content in content_list:
                        # time.sleep('%.2f' % random.random())
                        detail_rsp = p.apply_async(self.parse, args=(content['phone_goods_url'],))
                        detail_rsp = detail_rsp.get()
                        detail_info = self.get_detail_info(str(detail_rsp), content, info_list)
        p.close()
        p.join()


if __name__ == '__main__':
    ua = UserAgent()
    tm_spider = TmSpider()
    tm_spider.run()
