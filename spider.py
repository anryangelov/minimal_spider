
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from concurrent.futures import FIRST_COMPLETED

import requests
from lxml import etree


class Response():

    def __init__(self, req_resp):
        self._req_resp = req_resp
        self.text = req_resp.text
        self.url = req_resp.url
        self.root = etree.HTML(self.text)

    def xpath(self, xpath, *args, **kwargs):
        return self.root.xpath(xpath, *args, **kwargs)

    def __repr__(self):
        return 'Rspnonse({})'.format(self.url)


class BaseSpider:

    urls = []

    def __init__(
            self,
            concurent=64,
            connect_timeout=30,
            read_timeout=60
    ):
        self._pool = ThreadPoolExecutor(max_workers=concurent)
        self._futures = []
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout

    def parse(self):
        raise NotImplementedError(
            'parse method must be implement in child class')

    def _fetch(self, url, callback):
        response = requests.get(
            url, timeout=(self.connect_timeout, self.read_timeout))
        return Response(response), callback

    def follow(self, url, callback):
        future = self._pool.submit(self._fetch, url, callback)
        self._futures.append(future)

    def _handle_futures(self):
        while self._futures:
            done, not_done = wait(
                self._futures, timeout=60, return_when=FIRST_COMPLETED)
            for future in done:
                response, callback = future.result()
                generater = callback(response)
                for result in generater:
                    if isinstance(result, dict):
                        print(result)
                self._futures.remove(future)

    def run(self):
        for url in self.urls:
            self.follow(url, callback=self.parse)
            self._handle_futures()


class Spider(BaseSpider):

    urls = ['http://sportal.bg', 'http://topsport.bg']
    counter = 63

    def parse(self, response):
        while self.counter > 0:
            self.counter -= 1
            yield self.follow('http://abv.bg/', callback=self.parse)

        yield {'response': response}


if __name__ == '__main__':
    Spider().run()
