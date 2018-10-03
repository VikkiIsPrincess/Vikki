from link_crawler.utilities import orm_utility

__author__ = 'Wu Huanan'

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

    def parse(self, response):

        raise NotImplementedError()

    def spider_quit(self):
        """
        When the spider quit, release the resource it occupy.
            1. Release the selenium driver
        """
        self.quit_selenium_driver()

    def quit_selenium_driver(self):
        """
        quit the selenium driver.
        """
        if hasattr(self, SELENIUM_POOL):
            getattr(self, SELENIUM_POOL).quit()
            self.logger.warning("selenium driver quit.")

    @property
    def logger(self):

        return get_logger(self.name)

    @property
    def batch_id(self):

        return '{}-{}-{}'.format(self.site_mark,
                                 str(int(round(time.time() * 1000))),
                                 'LINK')
