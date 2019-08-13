# _*_ coding:utf-8 _*_
from fake_useragent import UserAgent
import requests
import datetime
import time
import aiohttp
import asyncio
import pymongo
import random
from async_retrying import retry
from requests.packages.urllib3.exceptions import InsecureRequestWarning

ua = UserAgent()
headers = {'accept': 'application/json',
           'accept-encoding': 'gzip, deflate, br',
           'accept-language': 'zh-CN,zh;q=0.9',
           'cache-control': 'no-cache, no-store, must-revalidate',
           # 'cookie': 'BIcookieID=57524c89-1c15-4cce-9c1c-18135f8d86e9; checkoutType2=4; _ga=GA1.2.1273593047.1537497584; rskxRunCookie=0; rCookie=jbp6bbhn2rahxocw0jnooc; aud=12e8a40682c62990cafb41a0c60a9d80; RES_TRACKINGID=975043370619460; _qubitTracker=ls3z0zbouj4-0jmbed79v-3e1zfww; session#1=lhgak0z1denol2ulgutsx20v; RecentView_FF=; m131Set=?qb_flag_true; qb_m133-show-once-143830=true; cto_lwid=8cc85114-1ad4-497c-9066-ca912f653ab0; _br_uid_2=uid%3D5565132416009%3Av%3D11.8%3Ats%3D1540456173810%3Ahc%3D3; qb_m133-show-once-144127=true; _gcl_au=1.1.976150495.1550628582; grtng-mssg={"isToDisplay":true,"type":2}; ff_newsletter=1; usr-gender=249; ckm-ctx-sf=/cn; FF.Session=mzrngrgbbwiyiwq4mapx2zej; ub=DD757411655313368576C12809F2D3CF; FF.ApplicationCookie=bXGeY5mEod-dN8vfHJUj1kHtsJ2XMPmntpNTju1ienDT1B391cJQnppnrXVSfkfffAIcSYd_-IpQi5Ol-tvDMSScvVHYOROcmFc4JTaA502HCe6PZYPvC1vFzLbeS2fe6XZzdbsGnygO5fJbst9WK41gbYlWjkKAiucgVGeN-fVhF6Buh8y5FeflVfSmQWrQcEaAt9tYe-6_RA6oyCDvNihijzF-oPXoe2d2unwEURH8gg74; ABListing=122:1#149:1#334:0#335:0#336:1#338:0#340:1#341:0#342:1#345:1#346:1#35:2#61:1#64:1#77:2#81:1; ABLanding=139:0; ABCheckout=; ABReturns=; ABGeneral=1167:1#135:1#144:0#145:0#155:0#156:1#301:1#302:0#322:1#473:0#952:1; _gid=GA1.2.1792022358.1555551138; Hm_lvt_f2215c076b3975f65e029fad944be10a=1553571093,1553654404,1555551139; __jdv=206986628|direct|-|none|-|1555551139805; ResonanceSegment=; Hm_lpvt_f2215c076b3975f65e029fad944be10a=1555553481; _gat_UA-3819811-6=1; __jda=206986628.15374975847451253230103.1537497584.1555551139.1555553481.31; __jdb=206986628.1.15374975847451253230103|31.1555553481; RES_SESSIONID=993289891092735; lastRskxRun=1555553481845; _qst_s=29; _qsst_s=1555553482986; qb_session=1:0:17:DqWc=B:0:WouNokb:0:0:0:0:.farfetch.cn; qb_permanent=ls3z0zbouj4-0jmbed79v-3e1zfww:92:2:28:27:0::0:1:0:BbpFn1:Bct9zP:::::113.90.32.229:shenzhen:12628:china:CN:22.5431:114.098:shenzhen:156198:guangdong%20sheng:35592:migrated|1555553487617:DlJa=D1=B=BmLe=BL&DqWc=M9=C=BpwX=PZ:B:WouNosB:WouEu68:0:0:0::0:0:.farfetch.cn:0; ABProduct=77:2; __cid=81e4ba74-9363-421c-98b1-1953b2ec370f-219c5a3f0145479f619c163e',
           'x-requested-with': 'XMLHttpRequest',
           'user-agent': ua.random}

# 禁用安全请求警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


@retry(attempts=5)
# 获取网页（文本信息）
async def fetch(session, url):
    async with session.get(url, headers=headers) as response:
        return await response.json()


# 解析网页
async def parser(html):
    li_list = html.get('products')
    for li in li_list:
        item = {}
        item['brand_name'] = li.get('brand').get('name')
        item['pro_title'] = li.get('shortDescription')
        item['pro_website'] = 'https://www.farfetch.cn/cn/shopping/' + li.get('gender')+'/'+li.get('brand').get('name')+'-item-'+str(li.get('id'))+'.aspx?storeid='+str(li.get('merchantId'))
        item['pro_pic'] = li.get('images').get('cutOut')
        item['pro_price_new'] = li.get('priceInfo').get('finalPrice')
        item['pro_price_old'] = li.get('priceInfo').get('initialPrice')
        item['offers'] = item['pro_price_old'] - item['pro_price_new']
        item['discount'] = li.get('priceInfo').get('discountLabel')
        ran_time = random.randint(1, 1800)
        item['create_time'] = (datetime.datetime.now() + datetime.timedelta(seconds=ran_time)).strftime(
            "%Y-%m-%d %H:%M:%S")
        await save_mongodb(item)


# 存入mongodb数据库
async def save_mongodb(result):
    MONGO_DB = 'haitao'
    MONGO_TABLE = 'farfetch'
    client = pymongo.MongoClient(host='localhost', port=27017)
    db = client[MONGO_DB]
    collection = db[MONGO_TABLE]
    url_find = {'pro_website': result['pro_website']}
    if collection.find_one(url_find):
        pass
        # old_price = collection.find_one(url_find)['pro_price_new']
        # if float(result['pro_price_new']) <= float(old_price):
        #     print("***************旧数据，价格有所变动，直接删除后插入最新数据***************\n{}".format(result))
        #     collection.delete_one(url_find)
        #     collection.insert(dict(result))
    else:
        print("***************新数据，直接插入***************\n{}".format(result))
        collection.insert(dict(result))


# 处理网页
async def download(url):
    async with aiohttp.ClientSession() as session:
        html = await fetch(session, url)
        await parser(html)


def main():
    # 全部网页
    urls = []
    for i in range(1, 244):
        url_women = 'https://www.farfetch.cn/cn/plpslice/listing-api/products-facets?page={}&view=180&scale=315&pagetype=Shopping&gender=Women&pricetype=Sale'.format(i)
        urls.append(url_women)
    for j in range(1, 108):
        url_men = 'https://www.farfetch.cn/cn/plpslice/listing-api/products-facets?page={}&view=180&scale=282&pagetype=Shopping&gender=Men&pricetype=Sale'.format(j)
        urls.append(url_men)

    # 统计该爬虫的消耗时间
    print('#' * 50)
    t1 = time.time()  # 开始时间

    # 利用asyncio模块进行异步IO处理
    loop = asyncio.get_event_loop()
    tasks = [asyncio.ensure_future(download(url)) for url in urls]
    tasks = asyncio.gather(*tasks)
    loop.run_until_complete(tasks)

    t2 = time.time() # 结束时间
    print('使用aiohttp，总共耗时：%s' % (t2 - t1))
    print('#' * 50)


if __name__ == '__main__':
    main()