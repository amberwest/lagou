#! /usr/bin/env python3.5
# _*_ coding:utf-8 _*_
import multiprocessing

import re

from datetime import timedelta, datetime

crawlDate = datetime.now().strftime('%Y-%m-%d')


def process_salary(salary):
    """处理价格"""
    try:
        if 'k' and '-' in salary:
            nums = re.findall('(\d+)', salary)
            min_salary = int(nums[0]) * 1000
            max_salary = int(nums[1]) * 1000
            ave_salary = round((min_salary + max_salary) / 2)
            return min_salary, ave_salary, max_salary
    except:
        return 0, 0, 0


def process_datetime(date):
    """
    将发布时间处理成统一格式
    :param date: 有09:38，1天前，2017-11-01三种情况
    :return: 2017-11-01这种格式
    """
    if "天前" in date:
        day = re.findall('(\d+)天前', date)[0]
        now = datetime.now() - timedelta(days=int(day))
        new_date = now.strftime('%Y-%m-%d')
        return new_date

    elif ':' in date:
        return crawlDate
    else:
        return date