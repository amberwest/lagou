#! /usr/bin/env python3.5
# _*_ coding:utf-8 _*_
import pymongo
import pymysql
from pandas import DataFrame, ExcelWriter


class Lagoudb(object):
    def __init__(self, db_type='excel'):
        self.table = 'positions'
        self.db_type = db_type
        self.writer = ExcelWriter('./positions.xlsx')
        self.row = 0

    def create_conn(self):
        """根据选择的数据库类型来创建链接"""
        if self.db_type == 'mysql':
            self.conn = pymysql.connect(host='localhost', user='user', password='password', port=3306, db='lagou',
                                        charset='utf8')
        # 多进程情况下使用mongodb,要么在子进程建立后再初始化数据库连接,要么加上connect=false参数以防子进程复制父进程时死锁
        # 详情看链接 http://api.mongodb.com/python/current/faq.html#pymongo-fork-safe
        elif self.db_type == 'mongodb':
            self.client = pymongo.MongoClient(host='localhost', port=28001, document_class=dict, connect=False)
            self.db = self.client['lagou']
            self.collection = self.db['positins']

    def create_database_and_table(self):
        '''建立数据库和表格'''
        # print('创建数据库lagou')
        cursor = self.conn.cursor()
        # cursor.execute('DROP DATABASE IF EXISTS lagou')
        # cursor.execute('CREATE DATABASE lagou')
        cursor.execute('USE lagou')
        # 建立职位表格
        sql = """
        CREATE TABLE IF NOT EXISTS positions(
        positionId BIGINT(20) NOT NULL,
        crawlDate DATE DEFAULT NULL,
        createDate DATETIME DEFAULT NULL,
        positionName VARCHAR(100) DEFAULT NULL ,
        workYear VARCHAR (30) DEFAULT NULL ,
        education VARCHAR (20) DEFAULT NULL ,
        jobNature VARCHAR (20) DEFAULT NULL ,
        city VARCHAR (30) DEFAULT NULL ,
        min_salary VARCHAR (20) DEFAULT NULL ,
        ave_salary VARCHAR (20) DEFAULT NULL ,
        max_salary VARCHAR (20) DEFAULT NULL ,
        financeStage VARCHAR (200) DEFAULT NULL ,
        positionAdvantage VARCHAR (300) DEFAULT NULL ,
        industryField VARCHAR (100) DEFAULT NULL ,
        companyAddress VARCHAR (300) DEFAULT NULL ,
        companySize VARCHAR (20) DEFAULT NULL ,
        positionLabel VARCHAR (200) DEFAULT NULL ,
        companyName VARCHAR (100) DEFAULT NULL ,
        description text,
        PRIMARY KEY (positionId)
        )ENGINE=InnoDB DEFAULT CHARSET=utf8
        """
        # 手贱。。。
        # cursor.execute('DROP TABLE IF EXISTS positions')
        cursor.execute(sql)
        cursor.close()
        print('成功建立表格')

    def save_to_mysql(self, data):
        """更新并插入数据（单条）"""
        cursor = self.conn.cursor()
        # 必须保证列名和values后面的数据key对应，不然插入数据会对不上
        try:
            keys = ', '.join(data.keys())
            values = ', '.join(['%({key})s'.format(key=key) for key in keys.split(', ')])
            # insert ignore不更新数据，replace into则是先删除再插入，on duplicate key update则是更新重复数据(就是字典实现比较麻烦)
            sql = "INSERT IGNORE INTO {table}({keys}) VALUES ({values})".format(
                table=self.table, keys=keys, values=values)
            try:
                if cursor.execute(sql, data):
                    self.conn.commit()
                    print('成功插入数据到mysql')
            except:
                print('插入数据失败')
                self.conn.rollback()
                self.conn.close()
        except Exception as e:
            print(e)

    def save_to_mongodb(self, data):
        """将数据单条保存到mongodb"""
        try:
            print('成功插入数据到mongodb')
            self.collection.update({'positionId': data['positionId']}, {'$set': data}, upsert=True, multi=False)
        except:
            print('插入mongodb失败')

    def save_to_excel(self, data):
        """
        将数据保存到Excel
        :param data: 由单一条字典组成的列表
        :param row: 开始写入Excel的行号
        :return:
        """
        df = DataFrame(data=[data])
        if self.row == 0:
            header = True
            startrow = 0
        else:
            header = False
            startrow = self.row + 1
        df.to_excel(
            excel_writer=self.writer,
            header=header,
            index=False,
            startrow=startrow,
            sheet_name='positions'
        )
        self.writer.save()
        self.row += 1
        print('成功插入数据到Excel')

    def save_data(self, data):
        if self.db_type == 'mysql':
            self.save_to_mysql(data)
        elif self.db_type == 'mongodb':
            self.save_to_mongodb(data)
        else:
            self.save_to_excel(data)

    def shut_down_database(self):
        """关闭数据库连接"""
        if self.db_type == 'mysql':
            self.conn.close()
        elif self.db_type == 'mongodb':
            self.client.close()
