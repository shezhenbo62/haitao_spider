# -*- coding: utf-8 -*-
import scrapy
from jd_elect.items import JdElectItem
from urllib import parse
import re
import datetime
import random


class KaolaSpider(scrapy.Spider):
    name = 'kaola'
    allowed_domains = ['kaola.com']
    start_urls = ['https://search.kaola.com/category/1472.html?zn=top&zp=category-1-1-1',
                  'https://search.kaola.com/category/1471.html?zn=top&zp=category-1-1-2',
                  'https://search.kaola.com/category/1473.html?zn=top&zp=category-1-1-3',
                  'https://search.kaola.com/category/2881.html?zn=top&zp=category-1-1-4',
                  'https://search.kaola.com/category/6166.html?zn=top&zp=category-1-1-5',
                  ]

    def parse(self, response):
        li_list = response.xpath("//ul[@id='result']/li")
        for li in li_list:
            item = JdElectItem()
            # item = {}
            item['category'] = response.xpath("//a[@class='catCrumbs']/text()").extract_first()
            item['title'] = li.xpath(".//div[@class='titlewrap']/a/h2/text()").extract_first()
            item['goods_url'] = li.xpath(".//div[@class='titlewrap']/a/@href").extract_first()
            item['goods_url'] = parse.urljoin(response.url, item['goods_url'])
            item['img_url'] = 'https:' + li.xpath(".//div[@class='img']/img[1]/@data-src").extract_first()
            item['price'] = li.xpath(".//p[@class='price']/span[1]/text()").extract_first()
            item['price_ref'] = li.xpath(".//p[@class='price']/span[@class='marketprice']/del/text()").extract_first()
            data = li.xpath(".//p[@class='saelsinfo']")
            item['activity'] = data.xpath("string(.)").extract_first().replace('\n','').strip()
            item['comment_count'] = li.xpath(".//a[@class='comments']/text()").extract_first()
            item['shop_name'] = li.xpath(".//p[@class='selfflag']/span/text()").extract_first()
            yield scrapy.Request(item['goods_url'],
                                 self.get_goods_detail,
                                 meta={'item': item})

        # 翻页
        next_url = 'https:' + response.xpath("//a[text()='下一页']/@href").extract_first() if len(response.xpath("//a[text()='下一页']/@href"))>0 else None
        if next_url is not None:
            yield scrapy.Request(next_url,
                                 self.parse,
                                 meta={'item': item})

    def get_goods_detail(self,response):
        item = response.meta['item']
        item['brand'] = response.xpath("//dt[@class='orig-country']/a/text()").extract_first()
        item['sunburn_count'] = re.findall(r'"commentImagesCount":(\d+),',response.body.decode())[0] if len(re.findall(r'"commentImagesCount":(\d+),',response.body.decode()))>0 else None
        item['comment_grade'] = re.findall(r'"goodCommentsGrade":(\d+\.\d+),',response.body.decode())[0] + '%' if len(re.findall(r'"goodCommentsGrade":(\d+\.\d+),',response.body.decode()))>0 else None
        ran_time = random.randint(1, 3600)
        item['create_time'] = (datetime.datetime.now() - datetime.timedelta(seconds=ran_time)).strftime(
            "%Y-%m-%d %H:%M:%S")
        # item['create_time'] = '2019-04-20 09:00:00'
        yield item
