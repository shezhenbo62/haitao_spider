# from selenium import webdriver
# import time
# import pymongo
# from fake_useragent import UserAgent
# import pandas as pd
#
# MONGO_URL = 'localhost'
# MONGO_DB = 'd88_info'
# MONGO_TABLE = 'baiqiang'
# client = pymongo.MongoClient(MONGO_URL)
# db = client[MONGO_DB]
# ua = UserAgent()
#
#
# class Suning(object):
#     # 初始化时，传递要查询的关键词
#     def __init__(self):
#         pass
#
#     def suning(self):
#         option = webdriver.ChromeOptions()
#         # prefs = {"profile.managed_default_content_settings.images": 2}
#         # option.add_argument('user-agent="{}"'.format(ua.random))
#         # option.add_argument("--start-maximized")
#         # option.add_argument('--headless')
#         # option.add_extension(proxy_auth_plugin_path)
#         # option.add_experimental_option("prefs", prefs)
#
#         driver = webdriver.Chrome(chrome_options=option)
#         driver.get('https://www.qiang100.com/zhuanti/shouji.html')
#
#         # # 将滚动条移动到页面的底部
#         # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
#         # for i in range(0, 10000, 100):
#         #     driver.execute_script("window.scrollBy(0, {});".format(i))
#         #     time.sleep(1)
#         # driver.execute_script("document.body.scrollTop =0")
#         for _ in range(10):
#             driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
#             a = driver.find_element_by_xpath("//div[@class='brand-list-show-more']/span")
#             a.click()
#             time.sleep(2)
#         brand_list = driver.find_elements_by_xpath("//div[@class='special-list-in']/div")
#         url_list = []
#         for brand in brand_list:
#             detail_url = brand.find_element_by_xpath(".//h2/a").get_attribute('href')
#             url_list.append([detail_url])
#         df = pd.DataFrame(url_list, columns=['url'])
#         df.to_excel('C:/Users/Administrator/Desktop/phone_brand.xls', index=False)
#         # for url in url_list:
#         #     item = {}
#         #     driver.get(url)
#         #     item['img_url'] = driver.find_element_by_xpath("//div[@class='brand-detail clearfix fl']/div[1]/img").get_attribute('src')
#         #     item['brand_name'] = driver.find_element_by_xpath("//div[@class='brand-detail clearfix fl']/div[2]/h1").text.strip()
#         #     item['brand_info'] = driver.find_element_by_xpath("//div[@class='brand-info']/div[@class='intro']").text.strip()
#         #     self.save_mongodb(item)
#         driver.quit()
#
#     def save_mongodb(self,result):
#         try:
#             if db[MONGO_TABLE].insert(result):
#                 print('存储到mongodb成功', result)
#         except Exception:
#             print('存储到mongodb失败', result)
#
#
# if __name__ == '__main__':
#     sn = Suning()
#     sn.suning()


# import pymongo
# import pandas as pd
# import requests
# from fake_useragent import UserAgent
# from lxml import etree
#
# ua = UserAgent()
# MONGO_URL = 'localhost'
# MONGO_DB = 'd88_info'
# MONGO_TABLE = 'baiqiang'
# client = pymongo.MongoClient(MONGO_URL)
# db = client[MONGO_DB]
#
# headers = {
#     'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
#     'cache-control': 'max-age=0',
#     'accept-language': 'zh-CN,zh;q=0.9',
#     'upgrade-insecure-requests': '1',
#     'cookie': '__cfduid=dee4b30a34a08c239c8f51369706f42591551065128; PHPSESSID=lic2ib35dft818imhhtvutf2l0; UM_distinctid=16a4488b10757d-046c47af187798-e323069-1fa400-16a4488b10820f; bq_user_is_login=0; ta_clientId=4158aeaf39356078deee8c13211885a6; bq_page_tuiguang_trackId=bq_zhida_ziran-zhuanti_tag-%E6%89%8B%E6%9C%BA_1555927971686_1811333_77390060_106689; Hm_lvt_24b7d5cc1b26f24f256b6869b069278e=1556087043; rSeDyn_CXID=TtNJjwKo6Mkw7wc836qtvdAK; Hm_lvt_a49e3a6f14cabbf51b6b5046313a3c4c=1555927969,1556073716,1556087708,1556155431; ta_sessionId=729328884463ed4e7f79aa164ea26e36; ta_account=0; CNZZDATA1261416362=315635941-1555923702-%7C1556150998; Hm_lpvt_a49e3a6f14cabbf51b6b5046313a3c4c=1556155543; Qs_lvt_245553=1555927969%2C1556074705%2C1556155543; Qs_pv_245553=1543881549230248000%2C792566547590707700%2C1314992687614552600%2C2265065581480885800%2C2494391576277829600; check-adblock=1; ta_lastSendAt=1556155555881',
#     'User-Agent': ua.random}
# url_list = pd.read_excel('C:/Users/Administrator/Desktop/phone_brand.xls').values.tolist()
# for url in url_list:
#     response = requests.get(url[0], headers=headers)
#     if response.status_code == 200:
#         resp = response.content.decode()
#         html_resp = etree.HTML(resp)
#         item = {}
#         item['img_url'] = html_resp.xpath("//div[@class='brand-detail clearfix fl']/div[1]/img/@src")[0]
#         item['brand_name'] = html_resp.xpath("//div[@class='brand-detail clearfix fl']/div[2]/h1/text()")[0].strip()
#         item['brand_info'] = html_resp.xpath("//div[@class='brand-info']/div[@class='intro']/text()")[0].strip()
#         print(item)
#         db[MONGO_TABLE].insert(item)

# import re
#
# a = 'adidas阿迪达斯'
# b = 'adidas'
# c = 'adidas2'
# # d = re.findall(r'[a-zA-Z0-9\u4e00-\u9fff&\s]+', c)
# d = re.findall(r'(adidas1|2)', c)
# print(d)

import numpy as np
import datetime, time
import random

# data = list(np.arange(1, 1001))
# current_time = datetime.datetime.now()
# current_hour = current_time.hour
# # current_hour = 00
# time_frame = {21: 130, 22: 130, 15: 100, 16: 100, 9: 50, 10: 50, 19: 50, 20: 50, 23: 16, 0: 14, 1: 14, 2: 14,
#               3: 14, 4: 14, 5: 14, 6: 30, 7: 30, 8: 30, 11: 20, 12: 20, 13: 20, 14: 20, 17: 35, 18: 35}
# discount_num = time_frame.get(current_hour)
# result = data[0:discount_num]
# print(result)


# # 列表维度降维生成器
# def flatten(discount):
#     for each in discount:
#         if not isinstance(each, list):
#             yield each
#         else:
#             yield from flatten(each)
#
#
# if __name__ == "__main__":
#     discount = [['满&yen;100减&yen;50，满&yen;200减&yen;100', '每满&yen;100减&yen;50']]
#     discount2 = ['满&yen;100减&yen;50，满&yen;200减&yen;100', '每满&yen;100减&yen;50']
#     result = list(flatten(discount2))
#     print('，'.join(result).replace('&yen;', ''))


# -*- coding: utf-8 -*-
# from wxpy import *
#
# # 初始化机器人，扫码登陆，在命令行上登录console_qr参数必须设置为True
# # 因为在命令行登录不会弹出二维码图片，只会在命令行上直接生成二维码
# bot = Bot(console_qr=True)
# wechat_mps = bot.mps()
#
#
# # 打印来自其他好友、群聊和公众号的消息
# @bot.register(chats=wechat_mps)
# def print_others(msg):
#     print('msg:' + str(msg))
#     articles = msg.articles
#     if articles is not None:
#         for article in articles:
#             print('title:' + str(article.title))
#             print('summary:' + str(article.summary))
#             print('url:' + str(article.url))
#             print('cover:' + str(article.cover))
#
#
# if __name__ == '__main__':
#     # 或者仅仅堵塞线程
#     bot.join()

import requests
import re
from fake_useragent import UserAgent
from lxml import etree
from PIL import Image
from io import BytesIO


ua = UserAgent()
headers = {
    # 'Connection': 'keep-alive',
    # 'Upgrade-Insecure-Requests': '1',
    'User-Agent': ua.random,
    # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    # 'Accept-Encoding': 'gzip, deflate, br',
    # 'Accept-Language': 'zh-CN,zh;q=0.9',
}

# url = 'https://mp.weixin.qq.com/s/9nvOecXF9cS88rnNkjapcg'
# url = 'https://www.zara.cn/cn/'
# response = requests.get(url, headers=headers).text
# html = etree.HTML(response)

# # 匹配出id为js_content标签以下的文本内容，过滤掉公众号简介
# html_part = re.findall(r'id="js_content">[\s\S]+', response)[0]

# wechat_text = re.findall(r'[0-9\u4e00-\u9fff，.-]+', html_part)
# wechat_text = html.xpath("//div[@id='js_content']/descendant-or-self::span/text()")
# regex_content = re.compile(r'([a-zA-Z0-9\u4e00-\u9fff，%“”：。/|.、\s]*(?:限量|优惠|折|领券|促销)[a-zA-Z0-9\u4e00-\u9fff，%“”：。/|.、\s]*)')
# sales_info = regex_content.findall(response)
# sales_info = list(set(sales_info))
# wechat_img = re.findall(r'((https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|])', response)
# wechat_img = re.findall(r'((https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|])', response)
# text_list = [text for text in wechat_text if not text.isdigit()]
# img_urls = [img[0] for img in wechat_img if img[0].endswith(('jpeg', 'png', 'jpg'))]
# for img_url in img_urls:
#     response = requests.get(img_url)
#     tmpIm = BytesIO(response.content)
#     im = Image.open(tmpIm)
#     print(im.size)
#     print(sum(im.size))


# long_text = '，'.join(wechat_text)
# print(long_text)
# keywords = ['福利', '特惠', '抽奖', '折', '减', '优惠', '全场']
# # 文章文本中有活动关键字存在
# if any([keyword in long_text for keyword in keywords]):
#     print('*' * 100)
#     print('活动关键字存在，该文章正在做活动，文章地址为：{}'.format(url))
# # 不存在，下载图片并识别
# else:
#     print('请求图片')
# print(sales_info)
# print(img_urls)
#
#
# # 百度词法分析
# from aip import AipNlp
#
# APP_ID = '16195099'
# API_KEY = '8lI35u4np4CWmf3NNHhIK2do'
# SECRET_KEY = 'hVd7L1n23ftmc9WGlAgNWVCiUbDiP58j'
#
# client = AipNlp(APP_ID, API_KEY, SECRET_KEY)
# result = client.lexer(long_text.lower())
# items = result.get("items")
# for item in items:
#     print(item)

import cv2
import numpy as np


# img = cv2.imread('D:/yanzhengma/image (3).png', 0)
# print(img.shape)
# # img = cv2.medianBlur(img, 3)
# img = cv2.GaussianBlur(img, (5, 5), 0)
# th2 = cv2.adaptiveThreshold(img, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY, 11, 2)
# th1 = cv2.adaptiveThreshold(img, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2)
# ret1, th1 = cv2.threshold(img, 0, 255, cv2.THRESH_BINARY+cv2.THRESH_OTSU)

# cv2.imshow('normal', img)
# cv2.imshow('mean', th2)
# cv2.imshow('gauss', th1)
# cv2.imwrite('D:/yanzhengma/shibie2.png', th1)
# cv2.waitKey(0)
# cv2.destroyAllWindows()


# # # TODO  图像混合
# img2 = cv2.imread('C:/Users/Administrator/Desktop/test.png')  # 0为灰度图像
#
# img1 = cv2.imread('C:/Users/Administrator/Desktop/timg.jpg')
# # I want to put logo on top-left corner, So I create a ROI
# rows, cols, channels = img2.shape
# roi = img1[0:rows, 0:cols]
#
# # Now create a mask of logo and create its inverse mask also
# img2gray = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)
# ret, mask = cv2.threshold(img2gray, 10, 255, cv2.THRESH_BINARY)
# mask_inv = cv2.bitwise_not(mask)
#
# # Now black-out the area of logo in ROI
# img1_bg = cv2.bitwise_and(roi, roi, mask=mask_inv)
#
# # Take only region of logo from logo image.
# img2_fg = cv2.bitwise_and(img2, img2, mask=mask)
#
# # Put logo in ROI and modify the main image
# dst = cv2.add(img1_bg, img2_fg)
# img1[0:rows, 0:cols] = dst
#
# cv2.imshow('res', img1)
# cv2.imshow('huv', img2_fg)
# cv2.waitKey(0)
# cv2.destroyAllWindows()




# cv2.namedWindow('image', cv2.WINDOW_NORMAL)  # 可调整窗口大小
# cv2.imshow('image', img)
# cv2.waitKey(0)  # 参数为0，无限期等待键击
# cv2.destroyAllWindows()
# cv2.imwrite('messigray.jpg', img)  # 保存图像

# img = np.zeros((512, 512, 3), np.uint8)
#
# # 绘制线条
# img = cv2.line(img, (0, 0), (511, 511), (255, 0, 0), 5)  # 图像，线条的起点，终点，颜色，线条厚度
#
# # 绘制矩形
# img = cv2.rectangle(img, (384, 0), (510, 128), (0, 255, 0), 3)  # 矩形的左上角和右下角
#
# # 添加文字
# font = cv2.FONT_HERSHEY_SIMPLEX
# # 参数：图像，文字内容，位置，字体，文字大小，颜色，字体线条的粗细，字体外观
# cv2.putText(img, 'OpenCV', (10, 500), font, 4, (255, 255, 255), 2, cv2.LINE_AA)
#
# cv2.imshow('image', img)
# cv2.waitKey(0)  # 参数为0，无限期等待键击
# cv2.destroyAllWindows()

import requests
from lxml import etree
from fake_useragent import UserAgent
import pandas as pd
import pymongo
from bson import ObjectId
import time

MONGO_URL = 'localhost'
MONGO_DB = 'haitao'
MONGO_TABLE = 'yingshang2'
client = pymongo.MongoClient(MONGO_URL)
db = client[MONGO_DB]
collection = db[MONGO_TABLE]
ua = UserAgent()
# headers = {'User-Agent': ua.random}
# resp = requests.get('http://bizsearch.winshangdata.com/xiangmu.html', headers=headers).text
# html = etree.HTML(resp)
# li_list = html.xpath("//ul[@class='area-list fl w900']/li")[1:]
# table = []
# for li in li_list:
#     province = li.xpath("./a/text()")[0]
#     url = li.xpath("./a/@href")[0]
#     resp_area = requests.get(url, headers=headers).text
#     html_area = etree.HTML(resp_area)
#     li2_list = html_area.xpath("//div[@class='area-city w216']/ul/li")[1:]
#     for li2 in li2_list:
#         city = li2.xpath("./a/text()")[0] if len(li2.xpath("./a/text()")) > 0 else province
#         url_area = li2.xpath("./a/@href")[0] if len(li2.xpath("./a/@href")) > 0 else url
#         item = [province, url, city, url_area]
#         print(item)
#         table.append(item)
# df = pd.DataFrame(table, columns=['province', 'url', 'city', 'url_area'])
# df.to_excel('C:/Users/Administrator/Desktop/d88_yingshang.xls', index=False)

# df = pd.read_excel('C:/Users/Administrator/Desktop/d88_yingshang.xls')
# info_list = df.values.tolist()
# for info in info_list:
#     resp = requests.get(info[3], headers=headers).text
#     html = etree.HTML(resp)
#     li_list = html.xpath("//ul[@class='l-list clearfix']/li")
#     item = {}
#     for li in li_list:
#         item['_id'] = ObjectId()
#         item['province'] = info[0]
#         item['city'] = info[2]
#         item['mall_type'] = li.xpath("./@data-leixing")[0]
#         item['mall_name'] = li.xpath("./@data-name")[0]
#         print(item)
#         collection.insert(item)
#     next_url = 'http://bizsearch.winshangdata.com'+html.xpath("//a[text()='下一页']/@href")[0] if \
#         len(html.xpath("//a[text()='下一页']/@href")) > 0 else None
#     while next_url:
#         time.sleep(1)
#         resp_next = requests.get(next_url, headers=headers).text
#         html_next = etree.HTML(resp_next)
#         li2_list = html_next.xpath("//ul[@class='l-list clearfix']/li")
#         item = {}
#         for li2 in li2_list:
#             item['_id'] = ObjectId()
#             item['province'] = info[0]
#             item['city'] = info[2]
#             item['mall_type'] = li2.xpath("./@data-leixing")[0]
#             item['mall_name'] = li2.xpath("./@data-name")[0]
#             print(item)
#             collection.insert(item)
#         next_url = 'http://bizsearch.winshangdata.com'+html_next.xpath("//a[text()='下一页']/@href")[0] if \
#             len(html_next.xpath("//a[text()='下一页']/@href")) > 0 else None


# import time
#
# timeStamp = time.time()
# print(timeStamp)
# for i in range(16, 32):
#     a = time.mktime(time.strptime('2019-5-{} 0:00:00'.format(str(i)), "%Y-%m-%d %H:%M:%S"))
#     print(i, '：', a)


# import requests
#
# a = time.time()
# b = int(a*1000)
# print(a)
# print(b)
#
# headers = {
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0',
#     'Accept': '*/*',
#     'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
#     'Referer': 'http://www.gpsspg.com/bs.htm',
#     'Connection': 'keep-alive',
#     'Cookie': 'Hm_lvt_15b1a40a8d25f43208adae1c1e12a514=1558408173,1558408949; Hm_lpvt_15b1a40a8d25f43208adae1c1e12a514={}; ARRAffinity=f116114b631b918c421369fff6db64490a3eebc3d66f5b72db24b9145e8b399c'.format(int(a))
# }
#
# params = (
#     ('oid', '159'),
#     ('key', '8822xD8409A9337DC8578366AA3602761D275'),
#     ('output', 'jsonp'),
#     ('callback', 'jQuery110205492299932688803_{}'.format(str(int(b)))),
#     ('bs', '460,0,34860,62041'),
#     ('type', ''),
#     ('hex', '10'),
#     ('to', '2'),
#     ('_', '{}'.format(str(int(b)))),
# )
#
# response = requests.get('http://apis.gpsspg.com/bs/', headers=headers, params=params)
# print(response.text)

import itertools
import random
import pandas as pd


def shenzhen_map():
    a = 113.901368
    b = 114.242724
    c = b*1000000 - a*1000000
    print(c)
    longitude_list = []
    for i in range(0, int(c)+1):
        d = (a*1000000+i)/1000000
        longitude_list.append(d)
    print(longitude_list[:10])
    print(len(longitude_list))

    w = 22.572197
    k = 22.734424
    x = k*1000000 - w*1000000
    print(x)
    latitude_list = []
    for j in range(0, int(x)+1):
        y = (w*1000000+j)/1000000
        latitude_list.append(y)
    print(latitude_list[:10])
    print(len(latitude_list))

    result = list(itertools.product(random.sample(longitude_list, 6000), random.sample(latitude_list, 3000)))
    print(result[:10])
    print(len(result))

    random_result = random.sample(result, 2000000)
    print(random_result[:10])

    city_weizhi = pd.DataFrame(random_result, columns=['longitude', 'latitude'])
    city_weizhi.to_csv('C:/Users/Administrator/Desktop/map_position.csv', index=False)

# import hashlib
# import redis
#
# rediscli = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)
# client = pymongo.MongoClient('localhost')
# db = client['haitao']
# collection = db['dp_shop']
# df = pd.DataFrame(list(collection.find({})))
# df = df.drop(columns=['_id'])
# df['shop_id'] = df['shop_id'].astype(str)
# df['url'] = 'https://m.dianping.com/shop/' + df['shop_id']
# print(len(df))
# df = df.drop_duplicates('shop_id','first')
# print(len(df))
# df = df.dropna(how='all', subset=['area', 'category', 'address', 'dobusiness', 'phoneNum', 'thumbUrl'])
# df_list = df['url'].values.tolist()
# for url in df_list:
#     md5_url = hashlib.md5(url.encode('utf-8')).hexdigest()
#     confult = rediscli.sadd('dp_request_queue', md5_url)
#     print(confult)

# import redis
#
# rediscli = redis.StrictRedis(host='localhost', port=6379, db=0)
# cookies = rediscli.lrange('dp_cookies', 0, -1)
# print(len(cookies))
# print(cookies[-1].decode('utf-8'))
# rediscli.ltrim('dp_cookies', -3, -1)
# '__mta=216347305.1561034903867.1561034903867.1561034903867.1; cityid=7; _lxsdk_cuid=16b6f2ea3a3c8-088b4affd26b11-e343166-1fa400-16b6f2ea3a3c8; _lxsdk=16b6f2ea3a3c8-088b4affd26b11-e343166-1fa400-16b6f2ea3a3c8; _hc.v=fb324b35-1095-d4ee-0633-029eae23f274.1560938456; logan_custom_report=; switchcityflashtoast=1; dp_pwa_v_=cede7d743c680cf24367c7011286d76f3df2fce5; source=m_browser_test_33; default_ab=shop%3AA%3A5%7Cindex%3AA%3A1%7CshopList%3AC%3A4; logan_session_token=xn15r1aqg0dagx0o0a6q; citypinyin=shenzhen; cityname=5rex5Zyz; m_flash2=1; pvhistory=6L+U5ZuePjo8L2Vycm9yL2Vycm9yX3BhZ2U+OjwxNTYxMDMxNjU3NTc3XV9b; msource=default; cy=7; cye=shenzhen; _lxsdk_s=16b780bfa77-258-282-2c4%7C%7C145'



# sele_cookies = driver.get_cookies()
# cookies = {cookie.get('name'): cookie.get('value') for cookie in sele_cookies}
# dp_cookie = ''
# for k, v in cookies.items():
#     name = k + '=' + v + ';'
#     dp_cookie += name
# print(dp_cookie)

# from selenium import webdriver
# from fake_useragent import UserAgent
#
# ua = UserAgent()
# chromeOptions = webdriver.ChromeOptions()

# 设置代理
# chromeOptions.add_argument("--proxy-server=http://119.123.177.119:9000")
# chromeOptions.add_argument('user-agent="{}"'.format(ua.random))
# 一定要注意，=两边不能有空格，不能是这样--proxy-server = http://202.20.16.82:10152
# browser = webdriver.Chrome(chrome_options=chromeOptions)

# 查看本机信息，查看代理是否起作用
# browser.get("http://icanhazip.com")
# time.sleep(1)
# browser.get("http://m.dianping.com")
# browser.find_elements_by_xpath("//div[@class='Fix page']/a[8]")[0].click()
# time.sleep(1)
# browser.find_elements_by_xpath("//div[@class='shop-list']/a[1]")[0].click()
# sele_cookies = browser.get_cookies()
# cookies = {cookie.get('name'): cookie.get('value') for cookie in sele_cookies}
# dp_cookie = ''
# for k, v in cookies.items():
#     name = k + '=' + v + ';'
#     dp_cookie += name
# print(dp_cookie)
# browser.get('http://icanhazip.com')


import re
import requests

headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
           'Accept-Encoding': 'gzip, deflate, br',
           'Accept-Language': 'zh-CN,zh;q=0.9',
           'Cache-Control': 'max-age=0',
           'Connection': 'keep-alive',
           'Cookie': 'pgv_pvid=4535698504; pgv_pvi=9152003072; pac_uid=0_6ec3c73fbe29b; tvfe_boss_uuid=1ab31030987ad921; ua_id=PXWcE37497OaZwVTAAAAAP2bQ4EURrMJ2TV3wACj7-Y=; mm_lang=zh_CN; RK=WNRAkWjsUA; ptcz=a0f39d1062f7dd4e46ca766b7f13a683e7f7fbfa4d62ef10a4159d839b8bc114; xid=e9117bd08005d3539f4b81251ff9d889; ptui_loginuin=309600517; rewardsn=; wxtokenkey=777',
           'Host': 'mp.weixin.qq.com',
           'Upgrade-Insecure-Requests': '1',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}
url = 'https://mp.weixin.qq.com/s?__biz=MzIzODUzMjA1OA==&mid=2247485505&idx=1&sn=a4735025f9246a1c2a1241f4744406bb&chksm=e936acf4de4125e23f1675a4c5926d8243a92ef4d7f53154a518ed15f096bf7023e1e07b892f&scene=0&xtrack=1#rd'
# resp = requests.get(url, headers=headers).text
# # (?:pattern)非获取匹配
# regex_content = re.compile(r'([a-zA-Z0-9\u4e00-\u9fff，%“”.、\s]*(?:限量|优惠|折|领券)[a-zA-Z0-9\u4e00-\u9fff，%“”.、\s]*)')
# regex_time = re.compile(r'[即日起至]*?\d{1,4}[年月日号.\-/]+\d{1,2}[年月日号.\-/]+\d{0,2}[.至年月日号\-]?\d{0,4}[至年月日号.\-/]?\d{0,2}[年月日号.\-/]?\d{0,2}[年月日号.\-]?')
# sales_info = regex_content.findall(resp)  # 活动信息
# act_time = regex_time.findall(resp)  # 活动时间
# # 过滤掉act_time中的非活动时间字符
# time_list = []
# for i in act_time:
#     error_time = re.findall(r'\d+\.\d+\.\d+/', i)
#     if len(error_time) == 0:
#         time_list.append(i)
# print(time_list)
# print(len(sales_info))
# wechat_sales = [i.strip() for i in sales_info]
# wechat_sales = list(set(wechat_sales))
# result = '，'.join(wechat_sales)
# print(result)

# resp = '19.07.05至19.07.09'
# resp = '7.8-7.10'
# resp = '即日起至2019年8月25日'
# resp = '7月5日'
# resp = '7月2日-7月24日'
# resp = '即日起至7月4日'
# resp = '7/13-7/14'
# resp = '即日起-8.31'
# resp = '2019年7月5日-7月14日'
# resp = '2019年7月5日至7月31日'
# resp = '2019年7月5号'
# resp = '2019.07.11-2019.07.15'
# resp = '2019/07/11-07/15'
# resp = '2019/07/11-2019/07/15'
# regex_time = re.compile(r'[即日起至]*\d{1,4}[年月日号.\-/]+\d{1,2}[年月日号.\-/]+\d{0,2}[.至年月日号\-/]?\d{0,4}[至年月日号.\-/]?\d{0,2}[年月日号.\-/]?\d{0,2}[年月日号.\-]?')
# act_time = regex_time.findall(resp)
# print(act_time)

# str_list = ['7月12日至15日', '2019年7月10日', '7月18日-7月21日', '2019-07-10', '2019年7月13-14日', '即日起至2019年8月31日', '7月20号',
#      '7月13日-14日', '7/7-7/14', '7.2-7.29']
# time_list = []
# for i in str_list:
#     error_time = re.search(r'\d+[.\-年月/]\d+[.\-月号日]\d*/?[日号]?', i)
#     if error_time is not None:
#         if error_time.group() != i:
#             time_list.append(i)
# print(time_list)


