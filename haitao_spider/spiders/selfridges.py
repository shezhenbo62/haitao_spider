# -*- coding: utf-8 -*-
import scrapy
from haitao_spider.items import HaitaoSpiderItem
import datetime
from urllib import parse
import re
import json
import random


class BadeSpider(scrapy.Spider):
    name = 'self'
    allowed_domains = ['www.selfridges.com']
    start_urls = ['https://www.selfridges.com/CN/zh/cat/mens/on_sale/?cm_sp=MegaMenu-_-men-_-Sale',
                  'https://www.selfridges.com/CN/zh/cat/womens/on_sale/?cm_sp=MegaMenu-_-Women-_-Sale',
                  'https://www.selfridges.com/CN/zh/cat/shoes/on_sale/?cm_sp=MegaMenu-_-Shoes-_-Sale']

    def parse(self, response):
        item = HaitaoSpiderItem()
        div_list = response.xpath("//div[@data-item-primarykey='product-list']")
        # print(response.request.headers['User-Agent'])
        category = re.findall(r'cat/(\w+)/on_sale', response.url)[0]
        page_count = response.xpath("//div[@class='plp-listing-header']/div/@data-total-pages-count").extract_first()
        for div in div_list:
            item['brand_name'] = div.xpath(".//h4/a/text()").extract_first()  # 品牌名
            item['pro_title'] = div.xpath(".//h4/following-sibling::p/a/text()").extract_first()  # 标题
            item['pro_website'] = div.xpath(".//div[@class='richText-content ']/p/a/@href").extract_first()  # 详情页链接
            item['pro_website'] = parse.urljoin(response.url, item['pro_website'])
            item['pro_pic'] = div.xpath(".//div[@class='component-content left']/a/img/@src").extract_first()  # 图片链接
            if item['pro_pic']:
                item['pro_pic'] = 'https:' + item['pro_pic']
            item['pro_price_new'] = float(div.xpath(".//span[@class='now-price']/span/text()").extract_first().replace('¥','')) if len(div.xpath(".//span[@class='now-price']/span/text()"))>0 else None  # 最终价格
            item['pro_price_old'] = float(div.xpath(".//span[@class='was-price ']/span/text()").extract_first().replace('¥','')) if len(div.xpath(".//span[@class='was-price ']/span/text()"))>0 else None # 原价
            if item['pro_price_new'] and item['pro_price_old'] is not None:
                item['offers'] = item['pro_price_old'] - item['pro_price_new']  # 优惠金额
                item['discount'] = round(item['pro_price_new'] / item['pro_price_old'], 2)
            ran_time = random.randint(1, 1800)
            item['create_time'] = (datetime.datetime.now() + datetime.timedelta(seconds=ran_time)).strftime(
                "%Y-%m-%d %H:%M:%S")
            yield item

        # 翻页 改成了ajaxa异步加载
        # https://www.selfridges.com/api/cms/ecom/v1/CN/zh/productview/byCategory/byIds?ids=womens%7Con_sale&pageNumber=3&pageSize=60
        # https://www.selfridges.com/api/cms/ecom/v1/CN/zh/productview/byCategory/byIds?ids=mens%7Con_sale&pageNumber=2&pageSize=60
        # https://www.selfridges.com/api/cms/ecom/v1/CN/zh/productview/byCategory/byIds?ids=shoes%7Con_sale&pageNumber=2&pageSize=60
        headers = {'Api-Key': 'xjut2p34999bad9dx7y868ng'}
        for page in range(2, int(page_count)+1):
            next_url = 'https://www.selfridges.com/api/cms/ecom/v1/CN/zh/productview/byCategory/byIds?ids={}%7Con_sale&pageNumber={}&pageSize=60'.format(category, page)
            yield scrapy.Request(next_url,
                                 headers=headers,
                                 callback=self.parse_next,
                                 meta={'item': item})

    def parse_next(self, response):
        item = response.meta['item']
        js = json.loads(response.body.decode())
        for info in js['catalogEntryNavView']:
            item['brand_name'] = info.get('brandName')  # 品牌名
            item['pro_title'] = info.get('name')  # 标题
            item['pro_website'] = 'https://www.selfridges.com/CN/zh/cat/' + info.get('seoKey')  # 详情页链接
            item['pro_pic'] = 'https://images.selfridges.com/is/image/selfridges/' + info.get('imageName')  # 图片链接
            item['pro_price_new'] = float(info.get('price')[0].get('lowestPrice'))  # 最终价格
            item['pro_price_old'] = info.get('price')[0].get('lowestWasPrice')  # 原价
            if item['pro_price_old']:
                item['pro_price_old'] = float(item['pro_price_old'])
                item['offers'] = round(item['pro_price_old'] - item['pro_price_new'], 2)  # 优惠金额
                item['discount'] = round(item['pro_price_new'] / item['pro_price_old'], 2)
                ran_time = random.randint(1, 1800)
                item['create_time'] = (datetime.datetime.now() + datetime.timedelta(seconds=ran_time)).strftime(
                    "%Y-%m-%d %H:%M:%S")
                yield item