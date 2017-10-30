#!/bin/bash                                                                                                                                                                                      

cd /root/apps/scrapy/choice/
PATH=$PATH:/usr/local/bin
export PATH
scrapy crawl choiceBondSpider

