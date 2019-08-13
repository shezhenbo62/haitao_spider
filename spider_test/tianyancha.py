# coding:utf-8
import requests
import pymongo
from fake_useragent import UserAgent
from lxml import etree
import pandas as pd
import time
import random
from bson import ObjectId


def read_brand():
    MONGO_URL = 'localhost'
    MONGO_DB = 'haitao'
    MONGO_TABLE = 'D88_brand'
    client = pymongo.MongoClient(MONGO_URL)
    db = client[MONGO_DB]
    collection = db[MONGO_TABLE]
    df = pd.DataFrame(list(collection.find({})))
    brand_names = df[['brand_name', 'brand_name_change']].values.tolist()
    return brand_names


def fetch(url):
    ua = UserAgent()
    cookies = [
                # 'ssuid=8334736872; TYCID=3c3bd9a043f311e9b8ba9feb04e3b548; undefined=3c3bd9a043f311e9b8ba9feb04e3b548; _ga=GA1.2.720612173.1552304760; __insp_wid=677961980; __insp_nv=true; __insp_targlpu=aHR0cHM6Ly93d3cudGlhbnlhbmNoYS5jb20v; __insp_targlpt=5aSp55y85p_lLeWVhuS4muWuieWFqOW3peWFt1%2FkvIHkuJrkv6Hmga%2Fmn6Xor6Jf5YWs5Y_45p_l6K_iX_W3peWVhuafpeivol%2FkvIHkuJrkv6HnlKjkv6Hmga%2Fns7vnu58%3D; __insp_norec_sess=true; __insp_slim=1554947411117; _gid=GA1.2.562241964.1557995029; RTYCID=f0151bba5df84cffaa9d0faa3d2d0f37; CT_TYCID=8b5bfa360dcc442aab9579268c91ebad; aliyungf_tc=AQAAAFuR6yBhIwsA/GYmeOu3rIKJx5iS; bannerFlag=undefined; csrfToken=QeVUuaykLfyLgy4bn-7VUcAi; Hm_lvt_e92c8d65d92d534b0fc290df538b4758=1558002106,1558003531,1558056821,1558063205; tyc-user-info=%257B%2522claimEditPoint%2522%253A%25220%2522%252C%2522myAnswerCount%2522%253A%25220%2522%252C%2522myQuestionCount%2522%253A%25220%2522%252C%2522signUp%2522%253A%25220%2522%252C%2522explainPoint%2522%253A%25220%2522%252C%2522privateMessagePointWeb%2522%253A%25220%2522%252C%2522nickname%2522%253A%2522%25E5%25A5%25BD%25E5%25A5%25BD11%2522%252C%2522integrity%2522%253A%252214%2525%2522%252C%2522privateMessagePoint%2522%253A%25220%2522%252C%2522state%2522%253A%25220%2522%252C%2522announcementPoint%2522%253A%25220%2522%252C%2522isClaim%2522%253A%25220%2522%252C%2522vipManager%2522%253A%25220%2522%252C%2522discussCommendCount%2522%253A%25221%2522%252C%2522monitorUnreadCount%2522%253A%252211%2522%252C%2522onum%2522%253A%25220%2522%252C%2522claimPoint%2522%253A%25220%2522%252C%2522token%2522%253A%2522eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxNTEwMjc2ODQ1NSIsImlhdCI6MTU1ODA2NDk4OSwiZXhwIjoxNTg5NjAwOTg5fQ.LLEsWgrYXvIsTrvDkqLF0aGWwHplkDadkAbetPnUBZuKr597-pBO_qLVYup4iWuOZbBsJVvmKEMIgUnYlic34A%2522%252C%2522pleaseAnswerCount%2522%253A%25221%2522%252C%2522redPoint%2522%253A%25220%2522%252C%2522bizCardUnread%2522%253A%25220%2522%252C%2522vnum%2522%253A%25220%2522%252C%2522mobile%2522%253A%252215102768455%2522%257D; auth_token=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxNTEwMjc2ODQ1NSIsImlhdCI6MTU1ODA2NDk4OSwiZXhwIjoxNTg5NjAwOTg5fQ.LLEsWgrYXvIsTrvDkqLF0aGWwHplkDadkAbetPnUBZuKr597-pBO_qLVYup4iWuOZbBsJVvmKEMIgUnYlic34A; _gat_gtag_UA_123487620_1=1; Hm_lpvt_e92c8d65d92d534b0fc290df538b4758=1558065087; cloud_token=b8a67b9e43b148ae8fb974a4f6272cba; cloud_utm=7976ea3beddd41d8b89a7c47f7533ac0',
               # 'ssuid=8334736872; TYCID=3c3bd9a043f311e9b8ba9feb04e3b548; undefined=3c3bd9a043f311e9b8ba9feb04e3b548; _ga=GA1.2.720612173.1552304760; __insp_wid=677961980; __insp_nv=true; __insp_targlpu=aHR0cHM6Ly93d3cudGlhbnlhbmNoYS5jb20v; __insp_targlpt=5aSp55y85p_lLeWVhuS4muWuieWFqOW3peWFt1%2FkvIHkuJrkv6Hmga%2Fmn6Xor6Jf5YWs5Y_45p_l6K_iX_W3peWVhuafpeivol%2FkvIHkuJrkv6HnlKjkv6Hmga%2Fns7vnu58%3D; __insp_norec_sess=true; __insp_slim=1554947411117; aliyungf_tc=AQAAAFGfVCpI5wEAzftd3lCR0dCmLSv2; bannerFlag=undefined; csrfToken=yeZKadc_eDbippbS707I6NAo; _gid=GA1.2.1804276995.1558320393; Hm_lvt_e92c8d65d92d534b0fc290df538b4758=1558056821,1558063205,1558077429,1558320393; tyc-user-info=%257B%2522claimEditPoint%2522%253A%25220%2522%252C%2522myAnswerCount%2522%253A%25220%2522%252C%2522myQuestionCount%2522%253A%25220%2522%252C%2522signUp%2522%253A%25220%2522%252C%2522explainPoint%2522%253A%25220%2522%252C%2522privateMessagePointWeb%2522%253A%25220%2522%252C%2522nickname%2522%253A%2522%25E6%2588%2591%25E7%25A1%25AE%25E8%25AE%25A4%25E6%258C%2596%25E6%258E%2598%25E5%259D%258E%25E5%25A4%25A7%25E5%2593%2588%25E7%259C%258B%25E5%2588%25B0%2522%252C%2522integrity%2522%253A%252214%2525%2522%252C%2522privateMessagePoint%2522%253A%25220%2522%252C%2522state%2522%253A%25220%2522%252C%2522announcementPoint%2522%253A%25220%2522%252C%2522isClaim%2522%253A%25220%2522%252C%2522vipManager%2522%253A%25220%2522%252C%2522discussCommendCount%2522%253A%25220%2522%252C%2522monitorUnreadCount%2522%253A%25220%2522%252C%2522onum%2522%253A%25220%2522%252C%2522claimPoint%2522%253A%25220%2522%252C%2522token%2522%253A%2522eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxNTY5MjA4NDE3MiIsImlhdCI6MTU1ODMyMDQ2MCwiZXhwIjoxNTg5ODU2NDYwfQ.UQ6giFGAM9wRUcu-ssqMx3m7TIqkZGEU1PfQbqScVQvl5SYuKje6-KCs18VOuJzxL5-uDf2Bq8CFmtzlKvm0mg%2522%252C%2522pleaseAnswerCount%2522%253A%25220%2522%252C%2522redPoint%2522%253A%25220%2522%252C%2522bizCardUnread%2522%253A%25220%2522%252C%2522vnum%2522%253A%25220%2522%252C%2522mobile%2522%253A%252215692084172%2522%257D; auth_token=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxNTY5MjA4NDE3MiIsImlhdCI6MTU1ODMyMDQ2MCwiZXhwIjoxNTg5ODU2NDYwfQ.UQ6giFGAM9wRUcu-ssqMx3m7TIqkZGEU1PfQbqScVQvl5SYuKje6-KCs18VOuJzxL5-uDf2Bq8CFmtzlKvm0mg; _gat_gtag_UA_123487620_1=1; Hm_lpvt_e92c8d65d92d534b0fc290df538b4758=1558320568',
               'ssuid=8334736872; TYCID=3c3bd9a043f311e9b8ba9feb04e3b548; undefined=3c3bd9a043f311e9b8ba9feb04e3b548; _ga=GA1.2.720612173.1552304760; __insp_wid=677961980; __insp_nv=true; __insp_targlpu=aHR0cHM6Ly93d3cudGlhbnlhbmNoYS5jb20v; __insp_targlpt=5aSp55y85p_lLeWVhuS4muWuieWFqOW3peWFt1%2FkvIHkuJrkv6Hmga%2Fmn6Xor6Jf5YWs5Y_45p_l6K_iX_W3peWVhuafpeivol%2FkvIHkuJrkv6HnlKjkv6Hmga%2Fns7vnu58%3D; __insp_norec_sess=true; __insp_slim=1554947411117; _gid=GA1.2.1804276995.1558320393; RTYCID=67c0d62f32e442cdaf5f306d1cdf047a; CT_TYCID=6d1fd69f3be94ff5ac2e3899308048d8; aliyungf_tc=AQAAABVrIxFKjAAAK67Ycogl9OG5sfaN; bannerFlag=undefined; csrfToken=-ZCs1oXsn2BJJlyrxEYXsnY3; Hm_lvt_e92c8d65d92d534b0fc290df538b4758=1558063205,1558077429,1558320393,1558333049; tyc-user-info=%257B%2522claimEditPoint%2522%253A%25220%2522%252C%2522myAnswerCount%2522%253A%25220%2522%252C%2522myQuestionCount%2522%253A%25220%2522%252C%2522signUp%2522%253A%25220%2522%252C%2522explainPoint%2522%253A%25220%2522%252C%2522privateMessagePointWeb%2522%253A%25220%2522%252C%2522nickname%2522%253A%2522%25E7%2599%25BD%25E8%25B5%25B7%2522%252C%2522integrity%2522%253A%25220%2525%2522%252C%2522privateMessagePoint%2522%253A%25220%2522%252C%2522state%2522%253A%25220%2522%252C%2522announcementPoint%2522%253A%25220%2522%252C%2522isClaim%2522%253A%25220%2522%252C%2522vipManager%2522%253A%25220%2522%252C%2522discussCommendCount%2522%253A%25220%2522%252C%2522monitorUnreadCount%2522%253A%25223%2522%252C%2522onum%2522%253A%25220%2522%252C%2522claimPoint%2522%253A%25220%2522%252C%2522token%2522%253A%2522eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxNzc2NDE4NzcwMSIsImlhdCI6MTU1ODMzMzEwMywiZXhwIjoxNTg5ODY5MTAzfQ.sBEBqmdfOQaH-Ar1nItsFO-q004_jyQm2BV7ZzjADA9em0csy_CNmNtTNrmmYQiWB0SXU0VLNCS9YjJ_R88GMw%2522%252C%2522pleaseAnswerCount%2522%253A%25220%2522%252C%2522redPoint%2522%253A%25220%2522%252C%2522bizCardUnread%2522%253A%25220%2522%252C%2522vnum%2522%253A%25220%2522%252C%2522mobile%2522%253A%252217764187701%2522%257D; auth_token=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxNzc2NDE4NzcwMSIsImlhdCI6MTU1ODMzMzEwMywiZXhwIjoxNTg5ODY5MTAzfQ.sBEBqmdfOQaH-Ar1nItsFO-q004_jyQm2BV7ZzjADA9em0csy_CNmNtTNrmmYQiWB0SXU0VLNCS9YjJ_R88GMw; Hm_lpvt_e92c8d65d92d534b0fc290df538b4758=1558333106',
               # 'ssuid=8334736872; TYCID=3c3bd9a043f311e9b8ba9feb04e3b548; undefined=3c3bd9a043f311e9b8ba9feb04e3b548; _ga=GA1.2.720612173.1552304760; __insp_wid=677961980; __insp_nv=true; __insp_targlpu=aHR0cHM6Ly93d3cudGlhbnlhbmNoYS5jb20v; __insp_targlpt=5aSp55y85p_lLeWVhuS4muWuieWFqOW3peWFt1%2FkvIHkuJrkv6Hmga%2Fmn6Xor6Jf5YWs5Y_45p_l6K_iX_W3peWVhuafpeivol%2FkvIHkuJrkv6HnlKjkv6Hmga%2Fns7vnu58%3D; __insp_norec_sess=true; __insp_slim=1554947411117; _gid=GA1.2.562241964.1557995029; RTYCID=f0151bba5df84cffaa9d0faa3d2d0f37; CT_TYCID=8b5bfa360dcc442aab9579268c91ebad; aliyungf_tc=AQAAAFuR6yBhIwsA/GYmeOu3rIKJx5iS; bannerFlag=undefined; csrfToken=QeVUuaykLfyLgy4bn-7VUcAi; Hm_lvt_e92c8d65d92d534b0fc290df538b4758=1558002106,1558003531,1558056821,1558063205; tyc-user-info=%257B%2522claimEditPoint%2522%253A%25220%2522%252C%2522myAnswerCount%2522%253A%25220%2522%252C%2522myQuestionCount%2522%253A%25220%2522%252C%2522signUp%2522%253A%25220%2522%252C%2522explainPoint%2522%253A%25220%2522%252C%2522privateMessagePointWeb%2522%253A%25220%2522%252C%2522nickname%2522%253A%2522%25E5%25AD%259F%25E4%25BC%25AF%25E9%25A3%259E%2522%252C%2522integrity%2522%253A%25220%2525%2522%252C%2522privateMessagePoint%2522%253A%25220%2522%252C%2522state%2522%253A%25220%2522%252C%2522announcementPoint%2522%253A%25220%2522%252C%2522isClaim%2522%253A%25220%2522%252C%2522vipManager%2522%253A%25220%2522%252C%2522discussCommendCount%2522%253A%25221%2522%252C%2522monitorUnreadCount%2522%253A%25220%2522%252C%2522onum%2522%253A%25220%2522%252C%2522claimPoint%2522%253A%25220%2522%252C%2522token%2522%253A%2522eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxMzc1MTY3ODU0MSIsImlhdCI6MTU1ODA2MzcyMCwiZXhwIjoxNTg5NTk5NzIwfQ.a_7FyAqbcGQXGsEiyTJsFzU75iwvo9NS8M2MIzCLrKSIKIq8bDpHLfmvVQ76AVBeAWK4IDq9AGXuM72gZJbEqQ%2522%252C%2522pleaseAnswerCount%2522%253A%25220%2522%252C%2522redPoint%2522%253A%25220%2522%252C%2522bizCardUnread%2522%253A%25220%2522%252C%2522vnum%2522%253A%25220%2522%252C%2522mobile%2522%253A%252213751678541%2522%257D; auth_token=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxMzc1MTY3ODU0MSIsImlhdCI6MTU1ODA2MzcyMCwiZXhwIjoxNTg5NTk5NzIwfQ.a_7FyAqbcGQXGsEiyTJsFzU75iwvo9NS8M2MIzCLrKSIKIq8bDpHLfmvVQ76AVBeAWK4IDq9AGXuM72gZJbEqQ; _gat_gtag_UA_123487620_1=1; Hm_lpvt_e92c8d65d92d534b0fc290df538b4758=1558063739',
               # 'ssuid=8334736872; TYCID=3c3bd9a043f311e9b8ba9feb04e3b548; undefined=3c3bd9a043f311e9b8ba9feb04e3b548; _ga=GA1.2.720612173.1552304760; __insp_wid=677961980; __insp_nv=true; __insp_targlpu=aHR0cHM6Ly93d3cudGlhbnlhbmNoYS5jb20v; __insp_targlpt=5aSp55y85p_lLeWVhuS4muWuieWFqOW3peWFt1%2FkvIHkuJrkv6Hmga%2Fmn6Xor6Jf5YWs5Y_45p_l6K_iX_W3peWVhuafpeivol%2FkvIHkuJrkv6HnlKjkv6Hmga%2Fns7vnu58%3D; __insp_norec_sess=true; __insp_slim=1554947411117; _gid=GA1.2.562241964.1557995029; RTYCID=f0151bba5df84cffaa9d0faa3d2d0f37; CT_TYCID=8b5bfa360dcc442aab9579268c91ebad; aliyungf_tc=AQAAAFuR6yBhIwsA/GYmeOu3rIKJx5iS; bannerFlag=undefined; csrfToken=QeVUuaykLfyLgy4bn-7VUcAi; Hm_lvt_e92c8d65d92d534b0fc290df538b4758=1558002106,1558003531,1558056821,1558063205; tyc-user-info=%257B%2522claimEditPoint%2522%253A%25220%2522%252C%2522myAnswerCount%2522%253A%25220%2522%252C%2522myQuestionCount%2522%253A%25220%2522%252C%2522signUp%2522%253A%25220%2522%252C%2522explainPoint%2522%253A%25220%2522%252C%2522privateMessagePointWeb%2522%253A%25220%2522%252C%2522nickname%2522%253A%2522%25E7%25A3%25A8%25E4%25BA%25BA%25E7%259A%2584%25E5%25B0%258F%25E6%2598%259F%25E6%2598%259F%2522%252C%2522integrity%2522%253A%252214%2525%2522%252C%2522privateMessagePoint%2522%253A%25220%2522%252C%2522state%2522%253A%25220%2522%252C%2522announcementPoint%2522%253A%25220%2522%252C%2522isClaim%2522%253A%25220%2522%252C%2522vipManager%2522%253A%25220%2522%252C%2522discussCommendCount%2522%253A%25220%2522%252C%2522monitorUnreadCount%2522%253A%25220%2522%252C%2522onum%2522%253A%25220%2522%252C%2522claimPoint%2522%253A%25220%2522%252C%2522token%2522%253A%2522eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxNzc5Nzc1NjU4MCIsImlhdCI6MTU1ODA2Mzc3NiwiZXhwIjoxNTg5NTk5Nzc2fQ.MWwK2MQtGHL0JYomZ_UhjA_WvFMdNd-VYiBt55bCyHfhNGKwzEnR4lmrJy9V2HL6eYIg1JB3tuk_SnWwOeNamw%2522%252C%2522pleaseAnswerCount%2522%253A%25220%2522%252C%2522redPoint%2522%253A%25220%2522%252C%2522bizCardUnread%2522%253A%25220%2522%252C%2522vnum%2522%253A%25220%2522%252C%2522mobile%2522%253A%252217797756580%2522%257D; auth_token=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxNzc5Nzc1NjU4MCIsImlhdCI6MTU1ODA2Mzc3NiwiZXhwIjoxNTg5NTk5Nzc2fQ.MWwK2MQtGHL0JYomZ_UhjA_WvFMdNd-VYiBt55bCyHfhNGKwzEnR4lmrJy9V2HL6eYIg1JB3tuk_SnWwOeNamw; _gat_gtag_UA_123487620_1=1; Hm_lpvt_e92c8d65d92d534b0fc290df538b4758=1558063799'
               ]
    # proxy_list = ['116.196.81.58:3128', '221.126.249.102:8080', '221.126.249.99:8080', '218.60.8.98:3129', '202.112.237.102:3128']
    # x = list(zip(proxy_list, cookies))
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Cookie': random.choice(cookies),
        'Host': 'www.tianyancha.com',
        # 'Referer': 'https://www.tianyancha.com/search?base=shenzhen&',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': ua.chrome}
    # response = requests.get(url, headers=headers, proxies={'https': random.choice(x)[0]})
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        print(response.status_code)


def parse(resp):
    html = etree.HTML(resp)
    url_list = html.xpath("//a[contains(@class, 'name')]/@href")[:3]
    print(len(url_list))
    return url_list


def get_content(resp, brand_name):
    html = etree.HTML(resp)
    item = {}
    item['_id'] = ObjectId()
    item['brand_name'] = brand_name
    item['company'] = html.xpath("//span[@class='detail']/@title")[0] if \
        len(html.xpath("//span[@class='detail']/@title")) > 0 else None
    item['status'] = html.xpath("//div[@class='tag-common -normal']/text()")[0] if \
        len(html.xpath("//div[@class='tag-common -normal']/text()")) > 0 else None
    item['phone_number'] = html.xpath("//span[@class='hidden']/text()")[0] if \
        len(html.xpath("//span[@class='hidden']/text()")) > 0 else None
    item['mail'] = html.xpath("//span[@class='email']/text()")[0] if \
        len(html.xpath("//span[@class='email']/text()")) > 0 else None
    item['legal_person'] = html.xpath("//div[@class='humancompany']/div[@class='name']/a/@title")[0] if \
        len(html.xpath("//div[@class='humancompany']/div[@class='name']/a/@title")) > 0 else None
    item['website'] = html.xpath("//a[@class='company-link']/@title")[0] if \
        len(html.xpath("//a[@class='company-link']/@title")) > 0 else None
    item['address'] = html.xpath("//script[@id='company_base_info_address']/text()")[0].strip() if \
        len(html.xpath("//script[@id='company_base_info_address']/text()")) > 0 else None
    print(item)
    save_mongodb(item)


def save_mongodb(item):
    MONGO_URL = 'localhost'
    MONGO_DB = 'd88_info'
    MONGO_TABLE = 'tianyancha_shenzhen'
    client = pymongo.MongoClient(MONGO_URL)
    db = client[MONGO_DB]
    collection = db[MONGO_TABLE]
    collection.insert(item)


def run():
    brand_names = read_brand()
    for brand in brand_names[190:200]:
        url = 'https://www.tianyancha.com/search?base=shenzhen&key={}'.format(brand[0])
        resp = fetch(url)
        time.sleep(float(format(random.uniform(3, 5), '.2f')))
        detail_urls = parse(resp)
        for detail_url in detail_urls:
            detail_resp = fetch(detail_url)
            get_content(detail_resp, brand[0])
            time.sleep(float(format(random.uniform(5, 10), '.2f')))


if __name__ == '__main__':
    run()
