# -*- coding: utf-8 -*-
import scrapy
from haitao_spider.items import HaitaoSpiderItem
import re, json, datetime
from urllib import parse
import random


class BadeSpider(scrapy.Spider):
    name = 'bade'
    allowed_domains = ['www.ba.de']
    start_urls = ['https://www.ba.de']

    def parse(self, response):
        url_list = response.xpath("//li[@class='nav-top']/a/@href").extract()[:-1]
        for url in url_list:
            yield scrapy.Request('https://www.ba.de' + url,
                                 callback=self.get_cate_url)

    def get_cate_url(self, response):
        item = HaitaoSpiderItem()
        item['category_id'] = re.findall(r'categoryId: (\d+),', response.body.decode())[0]
        item['category_name'] = re.findall(r'categoryName: "(.*?)",', response.body.decode())[0]
        category_url = 'https://www.ba.de/queryapi/lists?page=1&cid=%s&sort=top' % item['category_id']
        yield scrapy.Request(category_url,
                             self.get_info,
                             meta={'item': item})

    def get_info(self, response):
        item = response.meta['item']
        resp_json = json.loads(response.body.decode())
        results = resp_json.get('results')
        for result in results:
            item['brand_name'] = result.get('brand_name')
            item['pro_title'] = result.get('name')
            item['pro_website'] = parse.urljoin('https://www.ba.de/product/', result.get('url_path'))
            item['pro_pic'] = result.get('image_url')
            final_price = result.get('final_price')
            original_price = result.get('price')
            # item['metric_score'] = result.get('metric_score')
            # item['sales'] = result.get('sales')
            # item['total_sales'] = result.get('total_sales')
            item['pro_price_new'] = round(final_price * 7.613, 2)
            item['pro_price_old'] = round(original_price * 7.613, 2)
            item['offers'] = round(item['pro_price_old'] - item['pro_price_new'], 2)
            item['discount'] = round(item['pro_price_new']/item['pro_price_old'], 2)
            ran_time = random.randint(1, 1800)
            item['create_time'] = (datetime.datetime.now() + datetime.timedelta(seconds=ran_time)).strftime(
                "%Y-%m-%d %H:%M:%S")
            yield item

        # 翻页
        data_total = resp_json.get('pagination').get('total')
        page_num = data_total//40+1
        # category_id = response.meta['categoryId']
        for i in range(2, page_num+1):
            url = 'https://www.ba.de/queryapi/lists?page=%s&cid=%s&sort=top' % (i, item['category_id'])
            yield scrapy.Request(url,
                                 callback=self.get_info,
                                 meta={'item': item})




