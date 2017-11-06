#!/usr/bin/env python
# coding:utf-8
import re
import sys
import json
import os
import datetime
import MySQLdb
import requests
import multiprocessing
import logging
from retry import retry
from pymongo import MongoClient
import scrapy
from collections import Iterable
from scrapy.conf import settings
reload(sys)
sys.setdefaultencoding('utf8')

'''
同步数据
从mysql中获取所有secuList不完整的资讯id
从mongo中获取这些id对应的完整seculist
更新mysql
'''
class syscItem():

    def __init__(self):
        self.run()

    def run(self):
        choiceIdList = self.get_choice_list()
        try:
            count = len(choiceIdList)
            limit = 1000
            times = count//limit+1
            for i in range(times):
                where = {'choiceId':{'$in':choiceIdList}}
                result = self.getMongoItemsCommon(where,limit,i*limit)
                assert(isinstance(result,Iterable)==True)
                try:
                    for item in result:
                        secuList = json.dumps(item['secuList'])
                        updateItem = {'choiceId':item['choiceId']}
                        updateItem['secuList'] = secuList
                        updateItem['update_time'] = datetime.datetime.now()
                        self.updateMysql(updateItem)
                except:
                    print(sys.exc_info())
        except:
            print(sys.exc_info())
    #获取待下载的pdf数据
    def worker(self,num):
        #设置待下载的时间区间
        (leftDate,rightDate) = self.setDateRegion(num)
        #获取任务量
        count = self.getCount(leftDate,rightDate)
        limit = 1000
        times = count//limit+1
        for i in range(times):
            skip = i*limit
            items = self.getMongoItems(leftDate,rightDate,limit,skip) 
            for item in items:
                url = item['url']
                date = item['date']

                # #设置保存路径
                (relative_path,filename,filetype) = self.set_path(url,date,self.preDir)
	            #执行下载操作
                savepath = self.rootDir+relative_path
                sys.setdefaultencoding('utf8')   
                self.save_file(url,savepath)
               #下载成功后，更新数据状态
                updateItem = {'choiceId':item['choiceId']} 
                updateItem['file_path'] = relative_path
                updateItem['file_type'] = filetype
                updateItem['is_download'] = 1 
                updateItem['update_time'] = datetime.datetime.now()
                self.updateMongo(updateItem)
                self.updateMysql(updateItem)

    def getCount(self,leftDate,rightDate):
        collection = self.getMongoConn()
        return collection.find({"date" : { "$gte":leftDate, "$lt":rightDate},'is_download':{"$exists":False}}).count()          
    
    def getMongoItems(self,leftDate,rightDate,limit,skip):
        where = {"date" : { "$gte":leftDate, "$lt":rightDate},'is_download':{"$exists":False}}
        collection = self.getMongoConn()
        return collection.find(where).sort("date").limit(limit).skip(skip)

    def getMongoItemsCommon(self,where,limit,skip):
        collection = self.getMongoConn()
        return collection.find(where).sort("date").limit(limit).skip(skip)

    def updateMongo(self,updateItem): 
        collection = self.getMongoConn()
        is_success = collection.update({'choiceId':updateItem['choiceId']},{'$set':dict(updateItem)})['ok']  
        return is_success

    def getMongoConn(self):
        conn = MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )   
        db = conn[settings['MONGODB_DB']]
        collection = db[settings['MONGODB_COLLECTION']]
        return collection

    def get_conn(self):
        try:
            conn=MySQLdb.connect(
                    host=settings['MYSQL_SERVER'],
                    port=settings['MYSQL_PORT'],
                    user="root",
                    passwd="hoboom",
                    db=settings['MYSQL_DB'],
                    charset="utf8"
                )
            return conn
        except :
            logging.error("Database connect error:", sys.exc_info()[0])
            raise

    def get_choice_list(self):
        conn = self.get_conn()
        cursor = conn.cursor()
        sql = "SELECT choiceId from choice_news where secuList not like '%]'"
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            choiceIdList = []
            for item in result:
                choiceIdList.append(item[0])
            return choiceIdList
        except:
            print(sys.exc_info())
           # logging.error("Database connect error:", sys.exc_info()[0])
        conn.close()
        cursor.close()
           
    # 文件下载成功后，更新数据库状态
    def updateMysql(self,updateItem):
        conn = self.get_conn() 
        cursor = conn.cursor()
        sql = "update "+settings['MYSQL_TABLE'] + " set "
        setInfo = []
        for value in updateItem:
            if value == 'is_download':
                setStr = value + "=" + str(updateItem[value])
            else:
                setStr = value + "='" + str(updateItem[value]) + "'"
            setInfo.append(setStr)
        sql = sql + ','.join(setInfo)
        sql = sql + " where choiceId='"+str(updateItem['choiceId'])+"'"
        print(sql)
        try:
        # 执行SQL语句
            cursor.execute(sql)
            # 提交到数据库执行
            conn.commit()
        except:
        # 发生错误时回滚
            conn.rollback()
        cursor.close()
        conn.close() 

aa = syscItem()
