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
from scrapy.conf import settings
reload(sys)
'''
# 获取pdf下载链接，下载并保存到本地
1.从mongo中获取待下载的数据数量信息，根据日期分配进程
2.每个子进程从mongo中获取未下载成功的pdf链接
3.下载pdf
4.将pdf路径和是否下载成功的信息同时同步到mongo和MySQL
'''
class downloadFiles():
    rootDir = '/data/nfs/'
    preDir = 'bond/choice/'
    PROCESS_NUMBER = 12
    baseDate = '2015-01-01'
    #间隔，以月为单位
    size = 1 

    def __init__(self):
        jobs = []
        for i in range(self.PROCESS_NUMBER):
            p = multiprocessing.Process(target=self.worker, args=(i,)) 
            jobs.append(p)
            p.start()

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

    def setDateRegion(self,num):
        baseDateInfo = self.baseDate.split('-')
        leftMonth = int(baseDateInfo[1]) + num
        leftYear = int(baseDateInfo[0])
        if leftMonth>12:
            leftYear = leftYear + leftMonth//12
            leftMonth = leftMonth-12*(leftMonth//12)
        rightMonth = leftMonth + self.size
        rightYear = leftYear
        if rightMonth>12:
            rightYear = rightYear + rightMonth//12
            rightMonth = rightMonth - 12*(rightMonth//12)
        if leftMonth<10:
            leftMonth = '0'+str(leftMonth) 
        if rightMonth<10:
            rightMonth = '0'+str(rightMonth)
        leftDate = str(leftYear)+'-'+str(leftMonth)+'-'+baseDateInfo[2]
        rightDate = str(rightYear)+'-'+str(rightMonth)+'-'+baseDateInfo[2] 
        return (leftDate,rightDate)

    def getCount(self,leftDate,rightDate):
        collection = self.getMongoConn()
        return collection.find({"date" : { "$gte":leftDate, "$lt":rightDate},'is_download':{"$exists":False}}).count()          
    
    def getMongoItems(self,leftDate,rightDate,limit,skip):
        where = {"date" : { "$gte":leftDate, "$lt":rightDate},'is_download':{"$exists":False}}
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
        #设置文件保存的相对路径
    def set_path(self,url,date,pre=''):
        tail = 'default'
        dateArr = date.split('-')
        if dateArr:
            tail = '/'.join(dateArr)+'/'                 
        filename = url.split('/')[-1] 
        filetype = filename.split('.')[1] 
        relative_path = pre+tail+filename
        if not os.path.exists(self.rootDir+pre+tail):
            os.makedirs(self.rootDir+pre+tail)
        return (relative_path,filename,filetype)

    # 下载文件
    @retry(tries=5, delay=2)
    def save_file(self,url,path):
        #文件是否存在
        is_exist = os.path.exists(path) 
        if not is_exist:
            #若不存在，调用接口下载文件
            pdfResponse = requests.get(url, stream=True)
            if pdfResponse.status_code == 200:
                with open(path, "wb") as f:
                    for chunk in pdfResponse.iter_content(chunk_size=1024):
                        f.write(chunk)

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

aa = downloadFiles()
