# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import random
import time

from fake_useragent import UserAgent
from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
from scrapy.crawler import logger
from scrapy_redis import connection


class SpiderSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class SpiderDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class SpiderUserAgentMiddleware(SpiderDownloaderMiddleware):

    def __init__(self):
        self.user_agent = UserAgent(verify_ssl=False)
        self.data = None

    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def spider_opened(self, spider):
        self.data = self.user_agent.data_browsers
        spider.logger.info('SpiderUserAgentMiddleware is starting')

    def process_request(self, request, spider):
        rand_use = random.choice(
            self.data.get(random.choice(['chrome', 'opera', 'firefox', 'internetexplorer', 'safari'])))
        if rand_use:
            request.headers.setdefault('User-Agent', rand_use)
        return None


class SpiderProxyMiddleware(SpiderDownloaderMiddleware):

    def __init__(self, server, settings):
        self.server = server
        self.settings = settings

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        server = connection.from_settings(settings)

        s = cls(server, settings)
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(s.spider_closed, signal=signals.spider_closed)
        return s

    @staticmethod
    def spider_closed(spider):
        spider.logger.info('SpiderProxyMiddleware is closing')

    def process_request(self, request, spider):
        if self.settings.get('IS_PROXY'):
            proxy_ip = self._add_proxy()
            request.meta['proxy'] = proxy_ip
        return None

    def _add_proxy(self):
        while True:
            ip_str = self.server.lpop(self.settings.get('PROXY_REDIS_KEY'))
            time_sleep = 10
            if ip_str:
                break
            logger.info("未获取到代理ip,休眠{0}秒，等待下次获取~~~".format(time_sleep))
            time.sleep(time_sleep)
        proxy_ip = 'http://{0}'.format(ip_str)
        return proxy_ip

    def spider_opened(self, spider):
        spider.logger.info('SpiderProxyMiddleware is starting')
