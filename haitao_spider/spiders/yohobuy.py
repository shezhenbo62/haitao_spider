# -*- coding: utf-8 -*-
import scrapy
from haitao_spider.items import HaitaoSpiderItem
import datetime
import random


class BadeSpider(scrapy.Spider):
    name = 'yoho'
    allowed_domains = ['www.yohobuy.com']
    start_urls = ['https://www.yohobuy.com/boys-sale']

    def parse(self, response):
        # print(response.request.headers['User-Agent'])
        url_list = response.xpath("//div[@class='activity-entry clearfix']/a/@href").extract()[1:]
        for url in url_list:
            yield scrapy.Request(url,
                                 callback=self.get_info)

    def get_info(self, response):
        item = HaitaoSpiderItem()
        div_list = response.xpath("//div[@class='goods-container clearfix']/div")
        for div in div_list:
            item['brand_name'] = div.xpath("./div[last()]/p[@class='brand']/a/text()").extract_first()
            item['pro_title'] = div.xpath("./div[last()]/a/text()").extract_first()
            item['pro_pic'] = div.xpath(".//img/@data-original").extract_first()
            if item['pro_pic']:
                item['pro_pic'] = 'https:' + item['pro_pic'].split('?')[0]
            item['pro_website'] = div.xpath("./div[last()]/a/@href").extract_first()
            item['pro_price_new'] = div.xpath("./div[last()]/p[last()]/span[2]/text()").extract_first()
            item['pro_price_old'] = div.xpath("./div[last()]/p[last()]/span[1]/text()").extract_first()
            if item['pro_price_new'] and item['pro_price_old'] is not None:
                item['pro_price_new'] = float(item['pro_price_new'].replace("¥", ""))
                item['pro_price_old'] = float(item['pro_price_old'].strip().replace("¥", ""))
                item['offers'] = round(item['pro_price_old']-item['pro_price_new'], 2)
                item['discount'] = round(item['pro_price_new']/item['pro_price_old'], 2)
                ran_time = random.randint(1, 1800)
                item['create_time'] = (datetime.datetime.now() + datetime.timedelta(seconds=ran_time)).strftime(
                    "%Y-%m-%d %H:%M:%S")
                yield item

        # 翻页
        next_url = response.xpath("//a[@title='下一页']/@href").extract_first()
        if next_url is not None:
            if 'limit=200' in next_url:
                next_url = 'https://www.yohobuy.com' + next_url
            else:
                next_url = 'https://www.yohobuy.com' + next_url + '&limit=200'
            yield scrapy.Request(next_url,
                                 callback=self.get_info,
                                 meta={'item': item})
