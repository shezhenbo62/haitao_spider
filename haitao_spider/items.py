# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class HaitaoSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    category_id = scrapy.Field()
    category_name = scrapy.Field()
    brand_name = scrapy.Field()
    title = scrapy.Field()
    link = scrapy.Field()
    final_price = scrapy.Field()
    original_price = scrapy.Field()
    metric_score = scrapy.Field()
    sales = scrapy.Field()
    total_sales = scrapy.Field()
    rmb_final_price = scrapy.Field()
    rmb_original_price = scrapy.Field()
    offers = scrapy.Field()
    discount = scrapy.Field()
    shop_name = scrapy.Field()
    pro_title = scrapy.Field()
    pro_website = scrapy.Field()
    pro_price_new = scrapy.Field()
    pro_price_old = scrapy.Field()
    pro_pic = scrapy.Field()
    create_time = scrapy.Field()

