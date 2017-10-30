# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ChoiceBondItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    importLevel = scrapy.Field()
    choiceId = scrapy.Field()
    secuList = scrapy.Field()
    date = scrapy.Field()
    url = scrapy.Field()
    filetype = scrapy.Field()
    pagenum = scrapy.Field()
    filename = scrapy.Field()
    typeName = scrapy.Field()
    pageIndex = scrapy.Field()
    choiceOrder = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()
    pass

