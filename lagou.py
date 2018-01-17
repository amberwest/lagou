# /usr/bin/env python3.5
# -*- coding:utf-8 -*-
# 根据城市和职业对搜索到的职业信息进行爬取，并存到指定的数据库
# city：指定城市，默认为空
# kw: 指定职业，默认为空
# db_type: 指定保存数据的数据库，默认为excel，并以kw为名字保存到跟项目代码同一级目录。选项有mongodb和mqsql

import os

import pymongo
import pymysql
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import time

from pandas import DataFrame, ExcelWriter


class Lagou(object):
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Host': 'www.lagou.com',
        'Upgrade-Insecure-Requests': '1',
        'Connection': 'keep-alive',
        'Origin': 'www.lagou.com',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Anit-Forge-Code': '0',
        'X-Anit-Forge-Token': 'None',
    }

    def __init__(self, city, kw, db_type='excel'):
        self.table = 'jobs'
        self.city = city
        self.kw = kw
        self.crawlDate = str(time.strftime('%Y-%m-%d', time.localtime()))
        self.ua = UserAgent()
        self.maxRetryTime = 0
        path = os.path.join(os.path.dirname(__file__), self.kw + '.xlsx')
        self.writer = ExcelWriter(path)
        self.start_row = 0
        self.db_type = db_type

    def create_conn(self):
        """根据选择的数据库类型来创建链接"""
        if self.db_type == 'mysql':
            self.conn = pymysql.connect(host='localhost', user='user', password='password', port=3306, db='lagou', charset='utf8')
        elif self.db_type == 'mongodb':
            self.client = pymongo.MongoClient(host='localhost', port=28001, document_class=dict)
            self.db = self.client['lagou']
            self.collection = self.db['jobs']

    def getPositionInfo(self, pn=1):
        """通过关键字搜索得到职位信息列表"""
        if pn == 1:
            first = True
        else:
            first = False
        data = {
            'first': first,
            'kd': self.kw,
            'pn': str(pn)
        }
        Referer = 'https://www.lagou.com/jobs/list_{kw}?city={city}&cl={c1}&fromSearch=true&labelWords=&suginput'.format(
            kw=quote(self.kw), city=quote(self.city), c1='false')
        self.headers.update({'user-agent': self.ua.random, 'Referer': Referer})
        RequestsUrl = ('https://www.lagou.com/jobs/positionAjax.json?'
                       'city={city}&needAddtionalResult=false&isSchoolJob=0').format(city=quote(self.city))
        response = requests.post(RequestsUrl, data=data, headers=self.headers)
        print(response.url)
        info_json = response.json()
        msg = info_json.get('msg')

        if msg:
            # msg有内容则说明请求过于频繁
            print('重新爬取', RequestsUrl)
            if self.maxRetryTime < 6:
                self.maxRetryTime += 1
                time.sleep(10 * self.maxRetryTime)
                return self.getPositionInfo(pn)
            else:
                self.maxRetryTime = 0
                pn += 1
                return self.getPositionInfo(pn)
        else:
            print('正在爬取', RequestsUrl)
            results = info_json.get('content').get('positionResult').get('result')
            positionResults = []

            if results:
                print('这一页有%s条数据' % len(results))
                for i in range(len(results)):
                    # 如果前面有重复爬取但不够6次，这时maxRetryTime的值也被重写了，所以要设置为0
                    self.maxRetryTime = 0
                    position = {}

                    position['positionId'] = int(results[i].get('positionId'))
                    position['crawlDate'] = self.crawlDate
                    position['searchKeyword'] = self.kw
                    position['createDate'] = results[i].get('createTime')
                    position['positionName'] = results[i].get('positionName')
                    position['workYear'] = results[i].get('workYear')
                    position['education'] = results[i].get('education')
                    position['jobNature'] = results[i].get('jobNature')
                    position['city'] = results[i].get('city')
                    position['salary'] = results[i].get('salary')
                    position['financeStage'] = results[i].get('financeStage')
                    position['positionAdvantage'] = results[i].get('positionAdvantage')
                    position['industryField'] = results[i].get('industryField')
                    position['companyLabelList'] = ', '.join(results[i].get('companyLabelList'))
                    position['companySize'] = results[i].get('companySize')
                    position['positionLabel'] = ', '.join(results[i].get('positionLables'))
                    position['companyName'] = results[i].get('companyFullName')
                    posi_id = results[i].get('positionId')
                    position['description'] = self.getPositionDesc(posi_id)
                    positionResults.append(position)

                # print(positionResults)
                # 批量插入数据
                if self.db_type == 'mysql':
                    self.updateData(data=positionResults)
                elif self.db_type == 'mongodb':
                    self.saveToMongodb(data=positionResults)
                else:
                    # 默认存入excel
                    self.saveToExcel(data=positionResults, row=self.start_row)
                    # 更新写入excel的行号
                    self.start_row += len(positionResults)

                # 翻页
                pn += 1
                time.sleep(1)
                return self.getPositionInfo(pn)
            else:
                print('已经成功爬取了{0}的信息'.format(self.kw))
                return

    def getPositionDesc(self, positionId):
        """通过职位id重新请求来获取详细的职位要求及职责"""
        headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2)'
                          ' AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 '
                          'Safari/537.36',
        }
        detailUrl = 'https://m.lagou.com/jobs/{id}.html'.format(id=positionId)
        time.sleep(1)
        detail = requests.get(url=detailUrl, headers=headers)
        print(detail.url)
        try:
            soup = BeautifulSoup(detail.text, 'lxml')
            description = soup.select('.content')[0].get_text()
            return description
        except:
            if self.maxRetryTime < 6:
                self.maxRetryTime += 1
                print('第{0}次重新爬取{1}的信息'.format(self.maxRetryTime, positionId))
                time.sleep(10 * self.maxRetryTime)
                return self.getPositionDesc(positionId)
            elif self.maxRetryTime >= 6:
                self.maxRetryTime = 0
                print('爬取失败', detailUrl)
                return ''

    def createDatabaseAndTable(self):
        '''建立数据库和表格'''
        print('创建数据库lagou')
        # 新建并选择lagou数据库
        cursor = self.conn.cursor()
        cursor.execute('DROP DATABASE IF EXISTS lagou')
        cursor.execute('CREATE DATABASE lagou')
        cursor.execute('USE lagou')
        # 建立职位表格
        sql = """
        CREATE TABLE IF NOT EXISTS jobs(
        positionId BIGINT(20) NOT NULL,
        crawlDate DATE DEFAULT NULL,
        searchKeyword VARCHAR(30) DEFAULT NULL,
        createDate DATETIME DEFAULT NULL,
        positionName VARCHAR(100) DEFAULT NULL ,
        workYear VARCHAR (30) DEFAULT NULL ,
        education VARCHAR (20) DEFAULT NULL ,
        jobNature VARCHAR (20) DEFAULT NULL ,
        city VARCHAR (30) DEFAULT NULL ,
        salary VARCHAR (30) DEFAULT NULL ,
        financeStage VARCHAR (200) DEFAULT NULL ,
        positionAdvantage VARCHAR (300) DEFAULT NULL ,
        industryField VARCHAR (100) DEFAULT NULL ,
        companyLabelList VARCHAR (200) DEFAULT NULL ,
        companySize VARCHAR (20) DEFAULT NULL ,
        positionLabel VARCHAR (200) DEFAULT NULL ,
        companyName VARCHAR (100) DEFAULT NULL ,
        description text,
        PRIMARY KEY (positionId)
        )ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8
        """
        cursor.execute('DROP TABLE IF EXISTS jobs')
        cursor.execute(sql)
        cursor.close()
        print('成功建立表格')

    def updateData(self, data):
        """更新并插入数据"""
        cursor = self.conn.cursor()
        # 必须保证列名和values后面的数据key对应，不然插入数据会对不上
        keys = ', '.join(data[0].keys())
        values = ', '.join(['%({key})s'.format(key=key) for key in keys.split(', ')])
        # insert ignore不更新数据，replace into则是先删除再插入，on duplicate key update则是更新重复数据(就是字典实现比较麻烦)
        sql = "INSERT IGNORE INTO {table}({keys}) VALUES ({values})".format(
            table=self.table, keys=keys, values=values)
        try:
            if cursor.executemany(sql, data):
                self.conn.commit()
                print('成功插入数据')
        except:
            print('插入数据失败')
            self.conn.rollback()
            self.conn.close()

    def closeMysql(self):
        """关闭数据库"""
        self.conn.close()

    def saveToMongodb(self, data):
        """
        将数据批量保存到mongodb，这次不用update，尝试insert_many
        1、先创建unique 索引（主键）
        2、注意data的数据类型
        :param data: 由字典组成的列表
        :return:
        """
        try:
            print('插入数据到mongodb')
            self.collection.insert_many(documents=data, ordered=False)
        except:
            print('插入mongodb失败')

    def closeMongodb(self):
        """关闭mongodb数据库"""
        self.client.close()

    def saveToExcel(self, data, row):
        """
        将数据保存到Excel
        :param data: 由字典组成的列表
        :param row: 开始写入Excel的行号
        :return:
        """
        df = DataFrame(data=data)
        if row == 0:
            header = True
            startrow = 0
        else:
            header = False
            startrow = row + 1

        try:
            df.to_excel(
                excel_writer=self.writer,
                header=header,
                sheet_name=self.kw,
                startrow=startrow,
                index=False
            )
            self.writer.save()
            print('成功插入到excel')
        except:
            print('插入数据失败')

    def main(self):
        """主函数"""
        self.create_conn()
        self.getPositionInfo()
        if self.db_type == 'mysql':
            self.closeMysql()
        elif self.db_type == 'mongodb':
            self.closeMongodb()


if __name__ == '__main__':
    city = ''
    kw = '爬虫'
    type = 'mysql'
    lagou = Lagou(city=city, kw=kw, db_type=type)
    lagou.main()
