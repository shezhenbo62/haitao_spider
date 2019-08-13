# -*- coding: utf-8 -*-
import scrapy
from haitao_spider.items import HaitaoSpiderItem
import datetime
import random


class UnineedSpider(scrapy.Spider):
    name = 'unineed'
    allowed_domains = ['www.unineed.com']
    start_urls = ['https://www.unineed.com/sale.html']

    def parse(self, response):
        li_list = response.xpath("//dl[@id='narrow-by-list']/dd[last()]//li")
        for li in li_list:
            item = HaitaoSpiderItem()
            item['brand_name'] = li.xpath("./a/text()").extract_first().strip()
            url = li.xpath("./a/@href").extract_first()
            yield scrapy.Request(url,
                                 callback=self.get_info,
                                 meta={'item': item})

    def get_info(self, response):
        item = response.meta['item']
        li_list = response.xpath("//ul[@class='products-grid']/li")
        for li in li_list:
            item['pro_title'] = li.xpath(".//div[@class='item-info']//a/text()").extract_first()
            item['pro_website'] = li.xpath(".//div[@class='item-info']//a/@href").extract_first()
            item['pro_pic'] = li.xpath(".//div[@class='product-image']//img/@src").extract_first()
            item['pro_price_new'] = li.xpath(".//div[@class='item-content']//p[@class='special-price']/span[last()]/text()").extract_first()
            if item['pro_price_new']:
                item['pro_price_new'] = round(float(item['pro_price_new'].strip().replace('£', ''))*8.8542, 2)
                item['pro_price_old'] = round(float(li.xpath(
                    ".//div[@class='item-content']//p[@class='old-price']/span[last()]/text()"
                ).extract_first().strip().replace('£', ''))*8.8542, 2)
                item['offers'] = round(item['pro_price_old']-item['pro_price_new'], 2)
                item['discount'] = round(item['pro_price_new']/item['pro_price_old'], 2)
                ran_time = random.randint(1, 1800)
                item['create_time'] = (datetime.datetime.now() + datetime.timedelta(seconds=ran_time)).strftime(
                    "%Y-%m-%d %H:%M:%S")
                yield item

        next_url = response.xpath("//a[@class='button next i-next']/@href").extract_first()
        if next_url:
            yield scrapy.Request(next_url,
                                 callback=self.get_info,
                                 meta={'item': item})
