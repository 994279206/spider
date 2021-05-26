# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# useful for handling different item types with a single interface
import copy
import hashlib
import json
import time

import pymongo as pymongo
from scrapy_redis import connection
from twisted.internet.threads import deferToThread

from spider.default import DETAIL_TASK, LIST_TASK


class SpiderPipeline:
    def process_item(self, item, spider):
        return item


class SpiderRedisPipeline(SpiderPipeline):
    def __init__(self, server, slave_key, master_key, judge_key, scan_page):
        super(SpiderRedisPipeline, self).__init__()
        self.redis_server = server
        self.scan_page = False
        self._slave = slave_key
        self._master = master_key
        self._judge = judge_key
        self.scan_page = scan_page

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        slave_key = settings.get('REDIS_START_URLS_KEY')
        master_key = settings.get('REDIS_START_URLS_MASTER_KEY')
        judge_key = settings.get('REDIS_JUDGE_KEY')
        scan_page = settings.get('SCAN_PAGE')
        server = connection.from_settings(settings)
        s = cls(server, slave_key, master_key, judge_key, scan_page)
        return s

    @staticmethod
    def open_spider(spider):
        spider.logger.info('SpiderRedisPipeline is starting')

    def close_spider(self, spider):
        self.redis_server.close()
        spider.logger.info('SpiderRedisPipeline is closing')

    def process_item(self, item, spider):
        if spider.is_master:  # 主爬虫用于详情判重以及翻页
            return deferToThread(self.pro_items, item, spider)
        return item

    def pro_items(self, item, spider):
        """
        对列表结果数据的解析保存
        :param item:
        :param spider:
        :return:
        """
        try:
            task_item = spider._task
            detail_task_info = copy.deepcopy(task_item)  # 拷贝任务头信息
            list_task_info = copy.deepcopy(task_item)  # 拷贝任务头信息
            list_item = item
            detail_key, master_spider_key, judge_key = self.item_key(task_item, spider)
            detail_urls = list_item.get('detail_urls')
            next_page_url = list_item.get('next_page_url')
            if detail_urls and self.filter_items_url(detail_urls, detail_task_info,
                                                     master_spider_key, judge_key) and next_page_url:
                self.is_push_next_page_url(next_page_url, list_task_info, master_spider_key)
                spider.logger.info("[{1}]继续翻页，下一页：{0}".format(next_page_url, master_spider_key))
            del detail_task_info, list_task_info
            return item
        except Exception as e:
            spider.logger.error('RedisPipeline：{0}'.format(str(e)))

    def is_push_next_page_url(self, url, next_page_task, master_spider_key):
        """
        存入下一页
        :param url:
        :param next_page_task:
        :param master_spider_key:
        :return:
        """
        next_page_task['url'] = url  # 下一页翻页链接
        next_page_task['task_type'] = LIST_TASK  # 列表任务
        self.redis_server.rpush(master_spider_key, json.dumps(next_page_task))  # 翻页链接放入父爬虫采集入口

    def filter_items_url(self, detail_urls, task, detail_key, judge_key):
        """
        按照各个站点进行采集详情链接的过滤
        :param detail_urls:
        :param task:
        :param detail_key:
        :param judge_key:
        :return:
        """
        if not self.scan_page:
            new_url_count = 0
        else:
            new_url_count = 1
        for url in detail_urls:
            if url and not self.redis_server.hget(judge_key, url):  # 详情链接是否已经采集过
                task['url'] = url
                self.redis_server.hset(judge_key, url, int(time.time()))  # 详情链接加入判重队列
                new_url_count += 1
                task['task_type'] = DETAIL_TASK
                self.redis_server.rpush(detail_key, json.dumps(task))  # 存入详情采集任务
        return new_url_count

    def item_key(self, item, spider):
        """
        字符串格式化储存键和判重键
        :param item:
        :param spider:
        :return:
        """
        detail_key = self._slave % {'name': spider.name}  # 详情存储队列
        master_spider_key = self._master % {'name': spider.name}  # 父爬虫入口队列
        judge_key = self._judge % {'name': item.get('site_id')}  # 判重队列
        return detail_key, master_spider_key, judge_key


class SpiderMongoPipeline(SpiderPipeline):

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.client = None
        self.client_db = None
        self.conn = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'spider')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.client_db = self.client[self.mongo_db]
        spider.logger.info('SpiderMongoPipeline is staring')

    def close_spider(self, spider):
        self.client.close()
        spider.logger.info('SpiderMongoPipeline is closing')

    def process_item(self, item, spider):
        if not spider.is_master:
            task = spider._task
            self.insert_data(task, item)

    def insert_data(self, task, data):
        table_name = task.get('table')
        url = task.get('url', '')
        _id = self.md5_url_id(url)
        conn = self.client_db[table_name]
        conn.update_one({'_id': _id}, {"$set": data}, upsert=True)

    @staticmethod
    def md5_url_id(url):
        hash_md5 = hashlib.md5()
        hash_md5.update(url.encode('utf-8'))
        md5_id = hash_md5.hexdigest()
        return md5_id
