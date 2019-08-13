# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class JdElectItem(scrapy.Item):
    # define the fields for your item here like:
    title = scrapy.Field()
    price = scrapy.Field()
    goods_url = scrapy.Field()
    price_ref = scrapy.Field()
    brand = scrapy.Field()
    shop_name = scrapy.Field()
    comment_count = scrapy.Field()
    activity = scrapy.Field()
    activity1 = scrapy.Field()
    activity2 = scrapy.Field()
    activity3 = scrapy.Field()
    activity4 = scrapy.Field()
    activity5 = scrapy.Field()
    activity6 = scrapy.Field()
    activity_time = scrapy.Field()
    brand_name = scrapy.Field()
    brand_country = scrapy.Field()
    brand_url = scrapy.Field()
    img_url = scrapy.Field()
    sunburn_count = scrapy.Field()
    comment_grade =scrapy.Field()
    discount = scrapy.Field()

    website = scrapy.Field()
    story_url = scrapy.Field()
    brand_story = scrapy.Field()
    category = scrapy.Field()
    create_time = scrapy.Field()
    category_id = scrapy.Field()
