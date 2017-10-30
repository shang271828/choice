# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from pymongo import MongoClient
from scrapy.conf import settings
from scrapy.exceptions import DropItem
import mysql.connector
import logging 
import json

class MongoDBPipeline(object):
    
    def __init__(self):
        conn = MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        db = conn[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]

    def process_item(self, item, spider):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid :
            self.collection.update({'choiceId':item['choiceId']},{'$set':dict(item)},upsert=True,multi=True)
            logging.info(item['url'], exc_info=True)
        return item
    
class MysqlPipeline(object):

    def __init__(self):
        try:
            self.conn = mysql.connector.connect(
                user='root',
                password='hoboom',
                host = settings['MYSQL_SERVER'],
                database=settings['MYSQL_DB'],
                charset='utf8'
            )
            self.cur = self.conn.cursor(buffered=True)
        except Exception as e:                                                                                                                                 
            logging.error(e, exc_info=True)
            raise


    def __del__(self):
        self.cur.close()
        self.conn.close()

    def process_item(self, item, spider):
        try:
            #if spider.name == 'choiceBondSpider' or spider.name=='choiceBondGetSpider' :
            if self.is_new(item):  
                self.insert_data(item)
        except Exception as e:
            logging.error(e, exc_info=True)
            logging.error("error from question: ")
            raise
        return item

    def insert_data(self, item):  
        insertSql = """insert into """+settings['MYSQL_TABLE']+""" (%s) values ( %s )""" 
        item['secuList'] = json.dumps(item['secuList'])
        keys = item.fields.keys()    
        fields = u','.join(keys)    
        qm = u','.join([u'%s'] * len(keys))    
        sql = insertSql % (fields, qm)    
        data = [item[k] for k in keys]
        try:
            self.cur.execute(sql, data)
            self.conn.commit()
        except Exception as e:
            logging.error(e, exc_info=True)
            self.conn.rollback()
            raise

    def is_new(self, item):
        is_new = False
        sql = 'select * from '+settings['MYSQL_TABLE']+' where choiceId="'+item['choiceId']+'"'
        try:
            self.cur.execute(sql)
            result = self.cur.fetchall()
            if len(result) == 0:
                is_new = True
        except Exception as e:
            logging.error(e, exc_info=True)
        return is_new

