# Vikki
from link_crawler.repositories.url_redis_repo import UrlRedisRepo, UrlRepoType
from link_crawler.downloader_middlewares.selenium_grid_server_middleware import SELENIUM_POOL
from link_crawler.utilities.log_utility import get_logger
from scrapy import signals
from pydispatch import dispatcher
import scrapy
import time
from link_crawler.utilities.black_list import black_list


class BaseLinkSpider(scrapy.Spider):

    # need to overwrite these two fields
    site_mark = ''
    name = ''
    black_list = black_list

    def __init__(self):

        super(BaseLinkSpider, self).__init__()
        # reload url repository
        self.url_repo = UrlRedisRepo(spider_name=self.site_mark)
        old_url_empty = not self.url_repo.count(UrlRepoType.OLD)

        for (pid, url) in orm_utility.retrieve_crawled_urls(self.site_mark):
            self.url_repo.add(url, UrlRepoType.BASE)
            if old_url_empty:
                self.url_repo.add(url, UrlRepoType.OLD)
        dispatcher.connect(self.spider_quit, signals.spider_closed)

    def start_requests(self):

        raise NotImplementedError()
