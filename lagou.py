# _*_ coding:utf-8 _*_
# 爬取拉勾网的全部工作信息,从首页-->列表页-->详情页的顺序爬取，即是职业类别链接-->每个类别所有工作的id-->根据id爬取工作详情
# db_type: 指定保存数据的数据库，默认为excel。选项有mongodb、mysql和excel

import re

import requests
from datetime import datetime
import time

from multiprocessing import Pool, Manager
from scrapy import Selector

from Lagoudb import Lagoudb
from utils import process_salary, process_datetime


class Lagou(object):

    def __init__(self):
        self.crawlDate = datetime.now().strftime('%Y-%m-%d')

    def get_category_links(self):
        """获取职业类别链接"""
        print('开始爬取拉勾网首页的职业类别链接')
        category_links = set()
        url = 'https://www.lagou.com/'
        headers = {
            'Host': 'www.lagou.com', 'Pragma': 'no-cache', 'Upgrade-Insecure-Requests': '1',
            'Referer': 'https://www.lagou.com/',
            'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 '
                           '(KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'),
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            selector = Selector(text=response.text)
            categories = selector.css('.menu_box .menu_main .category-list')
            for category in categories:
                links = category.css('a::attr(href)')
                for link in links:
                    category_link = link.extract()
                    category_links.add(category_link)

            menu_subs = selector.css('.menu_sub.dn dd')
            for sub in menu_subs:
                links = sub.css('a::attr(href)')
                for link in links:
                    category_link = link.extract()
                    category_links.add(category_link)
            print(category_links)
            return category_links
        else:
            print('获取职业类别链接失败')

    def get_position_ids(self, url, queue=None, maxRetryTime=0):
        """获取所有工作链接的id，每个链接最多尝试6次，6次都失败就直接爬取下一页"""
        headers = {
            'Cookie': ('user_trace_token=20171106104237-263b3e0e-c29c-11e7-bb35-525400f775ce; '
                       'LGUID=20171106104237-263b410b-c29c-11e7-bb35-525400f775ce; '
                       'JSESSIONID=ABAAABAACBHABBI283591DE4AE4D3CB286709A81DF91DA8;'
                       ' index_location_city=%E5%85%A8%E5%9B%BD; _gat=1; SEARCH_ID=157f32a9698944709c743a9502d0b8ba; '
                       'X_HTTP_TOKEN=f55523493661c1433f9dfcc1fcb18d7f; _ga=GA1.2.252896556.1509936189; '
                       '_gid=GA1.2.637415877.1510537843; LGSID=20171118102757-16eb4236-cc08-11e7-9943-5254005c3644; '
                       'LGRID=20171118113145-006b32b5-cc11-11e7-96b5-525400f775ce; '
                       'Hm_lvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1509936189;'
                       ' Hm_lpvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1510975914; TG-TRACK-CODE=index_navigation'),
            'Host': 'www.lagou.com',
            'Referer': 'https://www.lagou.com/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/61.0.3163.100 Safari/537.36'),
        }
        position_ids = []
        res = requests.get(url, headers=headers)
        if res.url == url:
            selector = Selector(text=res.text)
            positions = selector.css('#s_position_list ul li div div .p_top a::attr(href)').extract()
            for position in positions:
                position_id = re.findall('https://www.lagou.com/jobs/(.*?).html', position)[0]
                queue.put(position_id)
                position_ids.append(position_id)
            print(position_ids)
            # 将id写入
            while not queue.empty():
                with open('./positionId.txt', 'a+') as f:
                    f.write(queue.get() + '\n')

            next_page = selector.xpath('//div[@class="pager_container"]/a[last()]/@href').extract_first(default='')
            print('next page', next_page)
            if 'https://www.lagou.com/zhaopin' in next_page and '404' not in next_page:
                return self.get_position_ids(url=next_page, queue=queue)
        else:
            if maxRetryTime < 6:
                maxRetryTime += 1
                print('第{0}次重新爬取'.format(maxRetryTime), url)
                time.sleep(10 * maxRetryTime)
                return self.get_position_ids(url=url, maxRetryTime=maxRetryTime)
            elif maxRetryTime >= 6:
                print('爬取失败', url)
                try:
                    # 尝试6次后爬取下一页
                    page_num = int(url.split('/')[-2]) + 1
                    next_page = re.sub('/(\d+)/', '/'+str(page_num)+'/', url)
                    return self.get_position_ids(next_page)
                except:
                    # 如果不能匹配出页数，url有可能是类别的首页，如果继爬取可能会是无限循环，所以舍弃
                    print('爬取失败', url)

    def get_position_info(self, positionId, maxRetryTime=0):
        """通过职位id重新请求来获取详细的职位要求及职责，maxRetryTime:以类内函数参数的形式出现，不然在多进程起不到作用"""
        # cookie很重要，一开始在浏览器选择了广州站，即使是全局爬取，获取到的数据仍然只有广州的数据
        detail_url = 'https://www.lagou.com/jobs/{id}.html'.format(id=str(positionId))
        headers = {
            'User-Agent': ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                           '(KHTML, like Gecko) Chrome/62.0.3202.89 Safari/537.36'),
            'Cookie': ('user_trace_token=20171106104237-263b3e0e-c29c-11e7-bb35-525400f775ce; '
                       'LGUID=20171106104237-263b410b-c29c-11e7-bb35-525400f775ce; '
                       'JSESSIONID=ABAAABAACBHABBI283591DE4AE4D3CB286709A81DF91DA8; _gat=1; '
                       'SEARCH_ID=a84e4e0d7db148b3ba71a3576f4dd95e; X_HTTP_TOKEN=f55523493661c1433f9dfcc1fcb18d7f; '
                       'index_location_city=%E5%85%A8%E5%9B%BD; _gid=GA1.2.637415877.1510537843;'
                       ' _ga=GA1.2.252896556.1509936189; LGSID=20171118102757-16eb4236-cc08-11e7-9943-5254005c3644; '
                       'LGRID=20171118110315-0577ec09-cc0d-11e7-96af-525400f775ce; '
                       'Hm_lvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1509936189; '
                       'Hm_lpvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1510974204; TG-TRACK-CODE=index_navigation'),
            'Referer': ('https://www.lagou.com/jobs/list_Python?city=%E5%85%A8%E5%9B%BD&'
                        'cl=false&fromSearch=true&labelWords=&suginput='),
            'Host': 'www.lagou.com',
            'X-Anit-Forge-Code': '0',
            'X-Anit-Forge-Token': None,
            'X-Requested-With': 'XMLHttpRequest'
        }
        time.sleep(0.5)
        detail = requests.get(url=detail_url, headers=headers)
        print('当前爬取链接', detail.url)
        if detail.status_code == 200:
            position = {}
            select = Selector(text=detail.text)
            description = select.css('dd.job_bt div').xpath('string(.)').extract_first().strip()
            job_request = select.xpath('//dd[@class="job_request"]/p')
            publish_time = select.css('.publish_time::text').extract_first().replace('\xa0 发布于拉勾网', '')
            address = ''.join(select.css('.work_addr').xpath('string(.)').extract())
            salary = process_salary(job_request.xpath('./span[1]/text()').extract_first(default=''))
            position['positionId'] = int(positionId)
            position['crawlDate'] = self.crawlDate
            position['createDate'] = process_datetime(publish_time)
            position['positionName'] = select.css('.job-name span::text').extract_first(default='')
            position['workYear'] = job_request.xpath('./span[3]/text()').extract_first('').replace('/', '')
            position['education'] = job_request.xpath('./span[4]/text()').extract_first('').replace('/', '')
            position['jobNature'] = job_request.xpath('./span[5]/text()').extract_first('').replace('/', '')
            position['city'] = job_request.xpath('./span[2]/text()').extract_first('').replace('/', '')
            position['min_salary'] = salary[0]
            position['ave_salary'] = salary[1]
            position['max_salary'] = salary[2]
            position['financeStage'] = ''.join(select.xpath('//ul[@class="c_feature"]/li[2]/text()').extract()).strip()
            position['positionAdvantage'] = select.css('.job-advantage p::text').extract_first('')
            position['industryField'] = ''.join(select.xpath('//ul[@class="c_feature"]/li[1]/text()').extract()).strip()
            position['companyAddress'] = re.sub('[\s+\n-]+', '', address).replace('查看地图', '')
            position['companySize'] = ''.join(select.xpath('//ul[@class="c_feature"]/li[3]/text()').extract()).strip()
            position['positionLabel'] = ','.join(select.css('ul.position-label li').xpath('string(.)').extract())
            position['companyName'] = select.css('h2.fl::text').extract_first().strip()
            position['description'] = description
            print(position)
            return position

        else:
            if detail.status_code == 301:
                print('该信息已被删除', detail_url)
                return ''
            elif maxRetryTime < 6:
                print(detail.url, detail.status_code)
                maxRetryTime += 1
                print('第{0}次重新爬取{1}的信息'.format(maxRetryTime, positionId))
                time.sleep(10 * maxRetryTime)
                return self.get_position_info(positionId, maxRetryTime=maxRetryTime)
            elif maxRetryTime >= 6:
                print('爬取失败', detail_url)
                return ''

    def get_all_position_ids(self):
        """保存所有职业信息id"""
        manager = Manager()
        queue = manager.Queue()
        pool = Pool(processes=10)
        categories = self.get_category_links()
        for link in categories:
            pool.apply_async(self.get_position_ids, args=(link, queue))
        pool.close()
        pool.join()

    def get_all_position_info(self):
        """获取所有工作详细信息"""
        # 将id去重
        all_position_ids = []
        with open('./positionId.txt', 'r') as f:
            ids = f.readlines()
            for id in ids:
                if id.strip() not in all_position_ids:
                    all_position_ids.append(id.strip('\n'))
        # 同样用了多进程，之前用队列在写入excel一直出错（数据库则正常），所以用callback参数，将结果直接返回到数据保存的函数
        pool = Pool(processes=10)
        for id in all_position_ids:
            pool.apply_async(self.get_position_info, args=(id, ), callback=database.save_data)
        pool.close()
        pool.join()

    def main(self):
        """主函数"""
        database.create_conn()
        # 建立表格
        database.create_database_and_table()
        self.get_all_position_ids()
        self.get_all_position_info()
        # 关闭数据库
        database.shut_down_database()


if __name__ == '__main__':
    t1 = time.time()
    db_type = 'mysql'
    database = Lagoudb(db_type=db_type)
    lagou = Lagou()
    lagou.main()
    t2 = time.time()
    print('total time', t2-t1)
