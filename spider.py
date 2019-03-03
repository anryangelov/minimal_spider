
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from concurrent.futures import FIRST_COMPLETED
from collections import namedtuple
import logging
import time

from requests import Request
from requests import RequestException
from requests_html import HTMLSession

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


FollowPage = namedtuple('FollowPage', ('request', 'callback', 'meta'))


class BaseSpider:

    urls = []
    pipelines = []

    max_concurrent_requests = 32
    connect_timeout = 10
    read_timeout = 30

    def __init__(self):
        self._pool = ThreadPoolExecutor(
            max_workers=self.max_concurrent_requests)
        self._futures = {}
        self.attempted_urls = set()

    def parse(self):
        raise NotImplementedError(
            'parse method must be implement in child class')

    def _fetch(self, request):
        session = HTMLSession()
        prepare_req = session.prepare_request(request)
        start = time.time()

        try:
            response = session.send(
                prepare_req,
                timeout=(self.connect_timeout, self.read_timeout))
            response.raise_for_status()
            end = time.time()
            logger.debug(
                'fetched: %s, time: %.2f, bytes: %s',
                response.url, end - start, len(response.content))
        finally:
            session.close()

        return response

    def follow(self, url, callback, meta=None):
        request = Request('GET', url)
        return FollowPage(request, callback, meta)

    def _submit_request(self, request, callback, meta):
        if request.url not in self.attempted_urls:
            future = self._pool.submit(self._fetch, request)
            self._futures[future] = (callback, meta)
            self.attempted_urls.add(request.url)

    def _handle_callback_result(self, callback, response, meta):
        generator = callback(response, meta)
        for result in generator:

            if isinstance(result, dict):
                for pipeline in self.pipelines:
                    if hasattr(pipeline, 'process_item'):
                        pipeline.process_item(result, self)

            elif isinstance(result, FollowPage):
                self._submit_request(*result)

            else:
                raise ValueError(
                    'method "{}" yield type "{}", allowed types are "{}" or "{}"'.format(
                        callback.__name__,
                        result.__class__.__name__,
                        dict.__name__,
                        FollowPage.__name__)
                )

    def _handle_futures_result(self):
        while self._futures:
            done, not_done = wait(
                self._futures, timeout=60, return_when=FIRST_COMPLETED)
            for future in done:

                try:
                    response = future.result()
                    callback, meta = self._futures[future]
                    self._handle_callback_result(callback, response, meta)
                except RequestException as e:
                    logger.warning('Problem accessing page: %s', str(e))

                self._futures.pop(future)

    def run(self):
        logger.info('spider started')
        start = time.time()

        for p in self.pipelines:
            if hasattr(p, 'start_spider'):
                p.start_spider(self)

        for url in self.urls:
            follow_page = self.follow(url, callback=self.parse)
            self._submit_request(*follow_page)
            self._handle_futures_result()

        for p in self.pipelines:
            if hasattr(p, 'stop_spider'):
                p.stop_spider(self)

        end = time.time()
        logger.info(
            'spider finished number urls attempted: %s, time: %.2f',
            len(self.attempted_urls), end - start)


def get_first(list_):
    return list_ and list_[0]


def get_from_trees(list_, xpath):
    for tree in list_:
        result = tree.xpath(xpath)
        if result:
            return get_first(result)
