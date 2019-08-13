# -*- coding: utf-8 -*-
import scrapy
import re
from jd_elect.items import JdElectItem
from copy import deepcopy
from urllib import parse
import json
import datetime
# from scrapy_redis.spiders import RedisSpider
# import io
# import sys
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='gbk')


class JddqSpider(scrapy.Spider):
    name = 'jdlk2'
    allowed_domains = ['www.jd.com','item.jd.com']
    start_urls = ['https://www.jd.com/allSort.aspx']
    # # 动态定义爬虫爬取域范围
    # def __init__(self, *args, **kwargs):
    #     domain = kwargs.pop('domain', '')
    #     self.allowed_domains = filter(None, domain.split(','))
    #     super(JddqSpider, self).__init__(*args, **kwargs)

    def parse(self, response):
        # 母婴
        li_list = response.xpath("//div[@class='col'][2]/div[@class='category-item m'][1]//dl/dd/a[position()<=3]")
        for li in li_list:
            b_href = li.xpath("./@href").extract_first()
            b_href = parse.urljoin(response.url, b_href)
            yield scrapy.Request(b_href,
                                 dont_filter=True,
                                 callback=self.get_content_info)

    def get_content_info(self,response):
        li_list = response.xpath("//ul[@class='gl-warp clearfix']/li")
        for li in li_list:
            item = JdElectItem()
            item['goods_url'] = li.xpath(".//div[@class='p-name']/a/@href").extract_first() # 商品详情链接
            item['goods_url'] = parse.urljoin(response.url, item['goods_url'])
            if len(li.xpath(".//div[contains(@class,'p-name')]/a/em/text()")) == 1:
                item['title'] = li.xpath(".//div[contains(@class,'p-name')]/a/em/text()").extract_first().replace('\n','').replace(' ','') # 商品标题
            elif len(li.xpath(".//div[contains(@class,'p-name')]/a/em/text()")) == 2:
                item['title'] = li.xpath(".//div[contains(@class,'p-name')]/a/em/text()").extract()[1].replace(
                    '\n', '').replace(' ', '')
            yield scrapy.Request(item['goods_url'],
                                 dont_filter=True,
                                 callback=self.get_detail_info,
                                 meta={'item': deepcopy(item)})

        # 下一页
        next_url = response.xpath("//a[@class='pn-next']/@href").extract_first()
        if next_url:
            next_url = parse.urljoin(response.url, next_url)
            yield scrapy.Request(next_url,
                                 dont_filter=True,
                                 callback=self.get_content_info,
                                 meta={'item': item})

    def get_detail_info(self,response):
        item = response.meta['item']
        try:
            cat = re.findall(r'cat: \[(.*?)\],', response.body.decode('gbk'))[0]
            skuId = re.findall(r'skuid: (\d+),', response.body.decode('gbk'))[0]
        except UnicodeDecodeError:
            print('Get UnicodeDecodeError')
            print(item['goods_url'])
            cat = re.findall(r'cat: \[(.*?)\],', response.body.decode('gb18030'))[0]
            skuId = re.findall(r'skuid: (\d+),', response.body.decode('gb18030'))[0]
            price_url = 'https://p.3.cn/prices/mgets?skuIds=' + skuId
            comment_url = 'https://club.jd.com/comment/productCommentSummaries.action?referenceIds=' + skuId
            activity_url = 'https://cd.jd.com/promotion/v2?skuId='+skuId+'&area=1_72_2799_0&cat='+cat
            item['img_url'] = response.xpath("//div[@id='preview']/div/img/@src").extract_first() \
                if len(response.xpath("//div[@id='preview']/div/img/@src"))>0 \
                else response.xpath("//div[@id='preview']/div/img/@data-origin").extract_first()
            item['img_url'] = parse.urljoin(response.url, item['img_url'])
            item['brand'] = response.xpath("//ul[@id='parameter-brand']/li/a/text()").extract_first() \
                if len(response.xpath("//ul[@id='parameter-brand']/li/a/text()"))>0 \
                else response.xpath("//ul[@class='parameter2']/li[3]/a/text()").extract_first()
            item['shop_name'] = response.xpath("//div[@class='name']/a/text()").extract_first() \
                if len(response.xpath("//div[@class='name']/a/text()"))>0 \
                else response.xpath("//div[@class='shopName']/strong/span/a/text()").extract_first()
            yield scrapy.Request(price_url,
                                 dont_filter=True,
                                 callback=self.get_goods_price,
                                 meta={'item': deepcopy(item),
                                       'url': comment_url,
                                       'activity_url': activity_url})
        else:
            price_url = 'https://p.3.cn/prices/mgets?skuIds='+skuId
            comment_url = 'https://club.jd.com/comment/productCommentSummaries.action?referenceIds='+skuId
            activity_url = 'https://cd.jd.com/promotion/v2?skuId=' + skuId + '&area=1_72_2799_0&cat=' + cat
            item['img_url'] = response.xpath("//div[@id='preview']/div/img/@src").extract_first() if len(
                response.xpath("//div[@id='preview']/div/img/@src")) > 0 \
                else response.xpath("//div[@id='preview']/div/img/@data-origin").extract_first()
            item['img_url'] = parse.urljoin(response.url, item['img_url'])
            item['brand'] = response.xpath("//ul[@id='parameter-brand']/li/a/text()").extract_first() if len(
                response.xpath("//ul[@id='parameter-brand']/li/a/text()")) > 0 \
                else response.xpath("//ul[@class='parameter2']/li[3]/a/text()").extract_first()
            item['shop_name'] = response.xpath("//div[@class='name']/a/text()").extract_first() if len(
                response.xpath("//div[@class='name']/a/text()")) > 0 \
                else response.xpath("//div[@class='shopName']/strong/span/a/text()").extract_first()
            yield scrapy.Request(price_url,
                                 dont_filter=True,
                                 callback=self.get_goods_price,
                                 meta={'item': deepcopy(item),
                                       'url': comment_url,
                                       'activity_url': activity_url})

    def get_goods_price(self,response):
        item = response.meta['item']
        comment_url = response.meta['url']
        activity_url = response.meta['activity_url']
        js_price = response.body.decode()
        ret = js_price.replace('\n','')
        res = json.loads(ret)
        item['price'] = res[0]['p'] # 商品价格
        yield scrapy.Request(comment_url,
                             dont_filter=True,
                             callback=self.get_comment_info,
                             meta={'item': item,
                                   'activity_url': activity_url})

    def get_comment_info(self,response):
        item = response.meta['item']
        activity_url = response.meta['activity_url']
        js_comment = response.body.decode('gbk')
        ret = json.loads(js_comment)
        item['comment_count'] = ret['CommentsCount'][0]['CommentCountStr'] # 评论数
        yield scrapy.Request(activity_url,
                             dont_filter=True,
                             callback=self.get_activity_info,
                             meta={'item': item})

    def get_activity_info(self,response):
        item = response.meta['item']
        try:
            js_activity = response.body.decode('gbk')
        except Exception as e:
            js_activity = response.body.decode('gb18030')
        # 优惠券活动
        quota = re.findall(r'"quota":(\d+),', js_activity)[0]\
            if len(re.findall(r'"quota":(\d+),', js_activity)) > 0 else None
        discount = re.findall(r'"discount":(\d+),', js_activity)[0]\
            if len(re.findall(r'"discount":(\d+),', js_activity)) > 0 else None
        if quota and discount:
            item['activity1'] = '满' + quota + '减' + discount
        # 下拉详情的所有活动
        lenth = len(re.findall(r'"content":"(.*?)",', js_activity))
        if lenth == 0:
            item['activity2'] = None
        elif lenth == 1:
            item['activity2'] = re.findall(r'"content":"(.*?)",', js_activity)[0]
        elif lenth == 2:
            item['activity2'] = re.findall(r'"content":"(.*?)",', js_activity)[0]
            item['activity3'] = re.findall(r'"content":"(.*?)",', js_activity)[1]
        elif lenth == 3:
            item['activity2'] = re.findall(r'"content":"(.*?)",', js_activity)[0]
            item['activity3'] = re.findall(r'"content":"(.*?)",', js_activity)[1]
            item['activity4'] = re.findall(r'"content":"(.*?)",', js_activity)[2]
        elif lenth == 4:
            item['activity2'] = re.findall(r'"content":"(.*?)",', js_activity)[0]
            item['activity3'] = re.findall(r'"content":"(.*?)",', js_activity)[1]
            item['activity4'] = re.findall(r'"content":"(.*?)",', js_activity)[2]
            item['activity5'] = re.findall(r'"content":"(.*?)",', js_activity)[3]
        # 赠送活动
        item['activity6'] = re.findall(r'"nm":"(.*?)",', js_activity)[0] \
            if len(re.findall(r'"nm":"(.*?)",', js_activity)) > 0 else None
        # 活动截止时间
        item['activity_time'] = re.findall(r'"timeDesc":"(.*?)",', js_activity)[0] \
            if len(re.findall(r'"timeDesc":"(.*?)",', js_activity)) > 0 else None
        item['create_time'] = datetime.date.today().strftime('%Y-%m-%d')
        yield item