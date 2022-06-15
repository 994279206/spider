#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time : 2021/4/15 16:29
# @Author : shl
# @File : main.py
# @Desc :
import importlib
import json

from scrapy import Request, signals
from scrapy_redis import defaults, connection
from scrapy_redis.spiders import RedisSpider
from scrapy_redis.utils import bytes_to_str


class Spider(RedisSpider):
    name = 'spider'

    def __init__(self, master=1):
        self._task = {}
        self._task_type = 0
        self.html = ''
        self.is_master = master
        self.fetch_data = None
        self._master()

    def _master(self):
        try:
            self.is_master = True if int(self.is_master) else False
        except ValueError:
            raise ValueError('启动参数master必须是0或者1,1:master,0:slave')

    def setup_redis(self, crawler=None):
        if self.server is not None:
            return

        if crawler is None:
            crawler = getattr(self, 'crawler', None)

        if crawler is None:
            raise ValueError("crawler is required")

        settings = crawler.settings

        if self.redis_key is None:
            self.redis_key = settings.get(
                'REDIS_START_URLS_KEY', defaults.START_URLS_KEY,
            )
        if self.is_master:
            self.redis_key = settings.get(
                'REDIS_START_URLS_MASTER_KEY', defaults.START_URLS_KEY,
            )

        self.redis_key = self.redis_key % {'name': self.name}

        if not self.redis_key.strip():
            raise ValueError("redis_key must not be empty")

        if self.redis_batch_size is None:
            self.redis_batch_size = settings.getint(
                'REDIS_START_URLS_BATCH_SIZE',
                settings.getint('CONCURRENT_REQUESTS'),
            )

        try:
            self.redis_batch_size = int(self.redis_batch_size)
        except (TypeError, ValueError):
            raise ValueError("redis_batch_size must be an integer")

        if self.redis_encoding is None:
            self.redis_encoding = settings.get('REDIS_ENCODING', defaults.REDIS_ENCODING)

        self.logger.info("Reading start URLs from redis key '%(redis_key)s' "
                         "(batch size: %(redis_batch_size)s, encoding: %(redis_encoding)s)",
                         self.__dict__)

        self.server = connection.from_settings(crawler.settings)

        if self.settings.getbool('REDIS_START_URLS_AS_SET', defaults.START_URLS_AS_SET):
            self.fetch_data = self.server.spop
            self.count_size = self.server.scard
        elif self.settings.getbool('REDIS_START_URLS_AS_ZSET', defaults.START_URLS_AS_ZSET):
            self.fetch_data = self.pop_priority_queue
            self.count_size = self.server.zcard
        else:
            self.fetch_data = self.pop_list_queue
            self.count_size = self.server.llen

        # The idle signal is called when the spider has no requests left,
        # that's when we will schedule new requests from redis queue
        crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(self.engine_started, signal=signals.engine_started)

    def engine_started(self):
        if self.is_master:
            self.logger.info("master spider being executed......")
        else:
            self.logger.info("slave spider being executed......")

    def make_request_from_data(self, data):
        """
        解析任务
        :param data: str redis任务数据
        :return:
        """
        data = bytes_to_str(data, self.redis_encoding)
        _data = json.loads(data)
        return self.make_requests_from_url(_data)

    def make_requests_from_url(self, _data):
        """
        重构请求
        :param _data: dict 任务信息
        :return:
        """
        url = _data.get('url', '')
        request = Request(url, meta={'task': _data}, dont_filter=True)
        return request

    def parse(self, response, **kwargs):
        print(response.text)
        self._init_info(response)
        data = self._do_task()
        yield data

    def _do_task(self):
        script_class = self.load_script_class()
        _class = script_class(self)
        if self._task_type == 0:
            data = self.parse_list(_class)
        else:
            data = self.parse_detail(_class)
        return data

    def _init_info(self, req):
        """
        初始任务数据信息
        :param req:
        :return:
        """
        meta_info = req.meta
        html_text = req.text
        self._task = meta_info.get('task', {})
        self._task_type = self._task.get('task_type', '')
        self.html = html_text

    @staticmethod
    def parse_list(_class):
        """
        列表信息的解析
        :param _class: 动态加载的类
        :return:
        """
        list_data = _class.parse_list()
        print('解析列表信息')
        return list_data

    @staticmethod
    def parse_detail(_class):
        """
        详细信息的解析
        :param _class: 动态加载的类
        :return:
        """
        print('解析详情信息')
        detail_data = _class.parse_detail()
        return detail_data

    @staticmethod
    def load_script(site_id, template_id):
        """
        动态加载模板脚本
        :param site_id:
        :param template_id:
        :return:
        """
        script_path = 'spider.script.s{0}_{1}'.format(site_id, template_id)
        module_script = importlib.import_module('.', script_path)
        importlib.reload(module_script)
        return module_script

    def load_script_class(self):
        """
        加载对应模板脚本的类
        :return:
        """
        class_name = 'Script'
        site_id = self._task.get('site_id')  # 网站配置的站点id
        template_id = self._task.get('template_id')  # 网站配置的站点采集数据类型id
        module = self.load_script(site_id, template_id)
        script_class = getattr(module, class_name)
        return script_class
