
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import time

import requests


class BaseSpider:

    urls = None

    def __init__(self, concurent=16):
        self.pool = ThreadPoolExecutor(max_workers=concurent)
        self.queue = Queue()
        self.items = {}

    def parse(self):
        raise NotImplementedError()

    def fetch(self, url, callback):
        print('in fetch', url, callback)
        response = requests.get(url)
        self.queue.put([response, callback])
        self.queue

    def follow(self, url, callback):
        print('in follow', url, callback)
        self.pool.submit(self.fetch, url, callback)

    def run(self):
        for url in self.urls:
            print('in run', url)
            self.follow(url, self.parse)
            for pair in self.queue.get():
                print('in run', pair)
                response, callback = pair
                callback(response)


class Spider(BaseSpider):

    urls = ['http://sportal.bg']

    def parse(self, response):
        print(response.status_code)


if __name__ == '__main__':
    Spider().run()
