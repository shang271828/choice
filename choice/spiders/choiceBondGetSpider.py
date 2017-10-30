#!/usr/bin/python                                                                                                           
#-*- encoding:utf-8 -*-
import scrapy
import logging
import json
from choice.items import ChoiceBondItem
import datetime
import re
import sys
reload(sys)

class ChoiceBondGetSpider(scrapy.Spider):
    name = "choiceBondGetSpider"
    start_urls = []

    def start_requests(self):
        sys.setdefaultencoding('utf8') 
        params = {'limit':'100'}
        params['sort'] = 'date'
        params['order'] = 'desc'
        typeList = ['F006']
        for page in range(0,8001):
            params['pageIndex'] = page
            print(page)
            for typeName in typeList:
                params['id'] = typeName
                base_url = 'http://app.jg.eastmoney.com/Notice/GetNoticeById.do'
                url = self.set_url(base_url,params)
                print(url)
                request_bond = scrapy.Request(url=url, callback=self.parse)
                yield request_bond

   
    def parse(self,response):
        result_json_string = response.body_as_unicode()
        url = response.url
        m= re.match(r".*id=(.*)", url)
        typeName = m.group(1) 
        m= re.match(r".*pageIndex=(.*)&limit=", url)
        pageIndex = m.group(1)
        try:
            result_json = json.loads(result_json_string)
            if result_json.has_key('records'):
                datas_json = result_json['records']
                for data in datas_json:
                    for attach in data['attach']:
                        item = ChoiceBondItem()
                        item['title'] = data['title']
                        item['importLevel'] = data['importLevel']
                        item['choiceId'] = data['id']
                        item['typeName'] = typeName
                        print(data['id'])
                        item['secuList'] = data['secuList']
                        item['url'] = attach['url']
                        item['filetype'] = attach['filetype']
                        item['pagenum'] = attach['pagenum']
                        item['filename'] = attach['name']
                        item['date'] = data['date']
                        item['choiceOrder'] = str(data['Order'])
                        item['pageIndex'] = pageIndex
                        item['create_time'] = str(datetime.datetime.now())
                        item['update_time'] = str(datetime.datetime.now())
                        yield item
        except Exception as e:  # not available site
            logging.error(e, exc_info=True)
            logging.error("Error process:"+response.url)

    def set_url(self,base_url,params):
        tmp = []
        for key in params:
            tmp.append(str(key)+'='+str(params[key]))
        return base_url+"?"+"&".join(tmp)
