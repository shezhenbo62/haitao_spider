# -*- coding: utf-8 -*-
import scrapy
import re
from jd_elect.items import JdElectItem
from copy import deepcopy
from urllib import parse
import json
import time
from scrapy_redis.spiders import RedisSpider
# import io
# import sys
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='gbk')


class JddqSpider(scrapy.Spider):
    name = 'jddq'
    allowed_domains = ['www.jd.com','item.jd.com']
    start_urls = ['https://www.jd.com/allSort.aspx']
    # name = "jddq"
    # # 启动爬虫的命令
    # redis_key = "jddqspider"
    #
    # # 动态定义爬虫爬取域范围
    # def __init__(self, *args, **kwargs):
    #     domain = kwargs.pop('domain', '')
    #     self.allowed_domains = filter(None, domain.split(','))
    #     super(JddqSpider, self).__init__(*args, **kwargs)

    def parse(self, response):
        # 家用电器url
        li_list = response.xpath("//div[@class='col'][1]/div[@class='category-item m'][4]//dl/dd/a")
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
            item['title'] = li.xpath(".//em/text()").extract_first().replace('\n','').replace(' ','') # 商品标题
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
            item['brand'] = response.xpath("//ul[@id='parameter-brand']/li/a/text()").extract_first()
            item['shop_name'] = response.xpath("//div[@class='name']/a/text()").extract_first()
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
            item['brand'] = response.xpath("//ul[@id='parameter-brand']/li/a/text()").extract_first()
            item['shop_name'] = response.xpath("//div[@class='name']/a/text()").extract_first()
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
        js_activity = response.body.decode('gbk')
        # print(js_activity)
        item['activity1'] = re.findall(r'"title":"(.*?)",', js_activity)[0] if len(re.findall(r'"title":"(.*?)",', js_activity)) > 0 else None
        item['activity2'] = re.findall(r'"content":"(.*?)",',js_activity)[0] if len(re.findall(r'"content":"(.*?)",',js_activity)) > 0 else None
        item['activity3'] = re.findall(r'"nm":"(.*?)",',js_activity)[0] if len(re.findall(r'"nm":"(.*?)",',js_activity)) > 0 else None
        print('数据保存成功%s' % time.time())
        yield item
