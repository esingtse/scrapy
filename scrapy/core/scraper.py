"""This module implements the Scraper component which parses responses and
extracts information from them"""

import logging
from collections import deque
from typing import Any, Deque, Iterable, Optional, Set, Tuple, Union

from itemadapter import is_item
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.python.failure import Failure

from scrapy import signals, Spider
from scrapy.core.spidermw import SpiderMiddlewareManager
from scrapy.exceptions import CloseSpider, DropItem, IgnoreRequest
from scrapy.http import Request, Response
from scrapy.utils.defer import defer_fail, defer_succeed, iter_errback, parallel
from scrapy.utils.log import failure_to_exc_info, logformatter_adapter
from scrapy.utils.misc import load_object, warn_on_generator_with_return_value
from scrapy.utils.spider import iterate_spider_output


QueueTuple = Tuple[Union[Response, Failure], Request, Deferred]


logger = logging.getLogger(__name__)


class Slot:
    """Scraper slot (one per running spider)"""

    MIN_RESPONSE_SIZE = 1024

    def __init__(self, max_active_size: int = 5000000):
        self.max_active_size = max_active_size
        self.queue: Deque[QueueTuple] = deque()
        self.active: Set[Request] = set()
        self.active_size: int = 0
        self.itemproc_size: int = 0
        self.closing: Optional[Deferred] = None

    def add_response_request(self, result: Union[Response, Failure], request: Request) -> Deferred:
        deferred = Deferred()
        self.queue.append((result, request, deferred))
        if isinstance(result, Response):
            self.active_size += max(len(result.body), self.MIN_RESPONSE_SIZE)
        else:
            self.active_size += self.MIN_RESPONSE_SIZE
        return deferred

    def next_response_request_deferred(self) -> QueueTuple:
        response, request, deferred = self.queue.popleft()
        self.active.add(request)
        return response, request, deferred

    def finish_response(self, result: Union[Response, Failure], request: Request) -> None:
        self.active.remove(request)
        if isinstance(result, Response):
            self.active_size -= max(len(result.body), self.MIN_RESPONSE_SIZE)
        else:
            self.active_size -= self.MIN_RESPONSE_SIZE

    def is_idle(self) -> bool:
        return not (self.queue or self.active)

    def needs_backout(self) -> bool:
        return self.active_size > self.max_active_size


class Scraper:
    """
    对spider中间件进行管理，通过中间件完成请求，响应，数据分析等工作

    对于一个下载的网页，会先调用各个spider中间件的'process_spider_input'方法处理，如果全部
    处理成功则调用request.callback或者spider.parse方法进行分析，然后将分析的结果调用各个spider中间件的‘process_spider_output'
    处理，都处理成功了再交给ItemPipeLine进行处理，ItemPipeLine调用定义的'process_item'处理爬取到的数据结果
    """

    def __init__(self, crawler):
        self.slot: Optional[Slot] = None
        self.spidermw = SpiderMiddlewareManager.from_crawler(crawler)
        itemproc_cls = load_object(crawler.settings['ITEM_PROCESSOR'])  # 从配置文件中获取ITEM_PROCESSOR，默认是scrapy.pipelines.ItemPipelineManager，其实背后也是一个中间件管理器，用于管理piplines
        self.itemproc = itemproc_cls.from_crawler(crawler)
        self.concurrent_items = crawler.settings.getint('CONCURRENT_ITEMS')  # 控制同时处理的爬取到的item的数据数目
        self.crawler = crawler
        self.signals = crawler.signals
        self.logformatter = crawler.logformatter

    @inlineCallbacks
    def open_spider(self, spider: Spider):
        """Open the given spider for scraping and allocate resources for it"""
        """声明了一个Slot,如果item管理器中的中间件定义了open_spider方法则调用它"""
        self.slot = Slot(self.crawler.settings.getint('SCRAPER_SLOT_MAX_ACTIVE_SIZE'))
        yield self.itemproc.open_spider(spider)

    def close_spider(self, spider: Spider) -> Deferred:
        """Close a spider being scraped and release its resources"""
        if self.slot is None:
            raise RuntimeError("Scraper slot not assigned")
        self.slot.closing = Deferred()
        self.slot.closing.addCallback(self.itemproc.close_spider)
        self._check_if_closing(spider)
        return self.slot.closing

    def is_idle(self) -> bool:
        """Return True if there isn't any more spiders to process"""
        return not self.slot

    def _check_if_closing(self, spider: Spider) -> None:
        assert self.slot is not None  # typing
        if self.slot.closing and self.slot.is_idle():
            self.slot.closing.callback(spider)

    def enqueue_scrape(self, result: Union[Response, Failure], request: Request, spider: Spider) -> Deferred:
        """把要分析的response放入自己的队列中，然后为这个response返回的deferred添加一个finish_scraping方法，用来处理scraping完成后的操作，然后调用_scrape_next处理队列中的response."""
        if self.slot is None:
            raise RuntimeError("Scraper slot not assigned")
        dfd = self.slot.add_response_request(result, request)

        def finish_scraping(_):
            self.slot.finish_response(result, request)
            self._check_if_closing(spider)
            self._scrape_next(spider)
            return _

        dfd.addBoth(finish_scraping)
        dfd.addErrback(
            lambda f: logger.error('Scraper bug processing %(request)s',
                                   {'request': request},
                                   exc_info=failure_to_exc_info(f),
                                   extra={'spider': spider}))
        self._scrape_next(spider)
        return dfd

    def _scrape_next(self, spider: Spider) -> None:
        """不断从队列中获取response来调用_scrape方法，并在_scrape后调用原来安装的finish_scraping方法"""
        assert self.slot is not None  # typing
        while self.slot.queue:
            response, request, deferred = self.slot.next_response_request_deferred()
            self._scrape(response, request, spider).chainDeferred(deferred)

    def _scrape(self, result: Union[Response, Failure], request: Request, spider: Spider) -> Deferred:
        """
        Handle the downloaded response or failure through the spider callback/errback
        """
        """该方法调用_scrape2后，会给deferred加入handle_spider_output方法，说明在_scrape2处理完成后会调用handle_spider_output方法"""
        if not isinstance(result, (Response, Failure)):
            raise TypeError(f"Incorrect type: expected Response or Failure, got {type(result)}: {result!r}")
        dfd = self._scrape2(result, request, spider)  # returns spider's processed output
        dfd.addErrback(self.handle_spider_error, request, result, spider)
        dfd.addCallback(self.handle_spider_output, request, result, spider)
        return dfd

    def _scrape2(self, result: Union[Response, Failure], request: Request, spider: Spider) -> Deferred:
        """
        Handle the different cases of request's result been a Response or a Failure
        """
        """判断如果request_result不是错误就调用spider中间件管理器的scrape_response方法"""
        if isinstance(result, Response):
            return self.spidermw.scrape_response(self.call_spider, result, request, spider)
        else:  # result is a Failure
            dfd = self.call_spider(result, request, spider)
            return dfd.addErrback(self._log_download_errors, result, request, spider)

    def call_spider(self, result: Union[Response, Failure], request: Request, spider: Spider) -> Deferred:
        """
        会对返回的response调用request对象内的callback或者spider.parse方法

        如果Request定义了callback则优先调用callback分析，如果没有则调用spider的parse方法分析
        """
        if isinstance(result, Response):
            if getattr(result, "request", None) is None:
                result.request = request
            callback = result.request.callback or spider._parse
            warn_on_generator_with_return_value(spider, callback)
            dfd = defer_succeed(result)
            dfd.addCallbacks(callback=callback, callbackKeywords=result.request.cb_kwargs)
        else:  # result is a Failure
            result.request = request
            warn_on_generator_with_return_value(spider, request.errback)
            dfd = defer_fail(result)
            dfd.addErrback(request.errback)
        return dfd.addCallback(iterate_spider_output)

    def handle_spider_error(self, _failure: Failure, request: Request, response: Response, spider: Spider) -> None:
        exc = _failure.value
        if isinstance(exc, CloseSpider):
            self.crawler.engine.close_spider(spider, exc.reason or 'cancelled')
            return
        logkws = self.logformatter.spider_error(_failure, request, response, spider)
        logger.log(
            *logformatter_adapter(logkws),
            exc_info=failure_to_exc_info(_failure),
            extra={'spider': spider}
        )
        self.signals.send_catch_log(
            signal=signals.spider_error,
            failure=_failure, response=response,
            spider=spider
        )
        self.crawler.stats.inc_value(
            f"spider_exceptions/{_failure.value.__class__.__name__}",
            spider=spider
        )

    def handle_spider_output(self, result: Iterable, request: Request, response: Response, spider: Spider) -> Deferred:
        if not result:
            return defer_succeed(None)
        it = iter_errback(result, self.handle_spider_error, request, response, spider)
        dfd = parallel(it, self.concurrent_items, self._process_spidermw_output,
                       request, response, spider)
        return dfd

    def _process_spidermw_output(self, output: Any, request: Request, response: Response,
                                 spider: Spider) -> Optional[Deferred]:
        """Process each Request/Item (given in the output parameter) returned
        from the given spider
        """
        assert self.slot is not None  # typing
        if isinstance(output, Request):
            self.crawler.engine.crawl(request=output)
        elif is_item(output):
            self.slot.itemproc_size += 1
            dfd = self.itemproc.process_item(output, spider)
            dfd.addBoth(self._itemproc_finished, output, response, spider)
            return dfd
        elif output is None:
            pass
        else:
            typename = type(output).__name__
            logger.error(
                'Spider must return request, item, or None, got %(typename)r in %(request)s',
                {'request': request, 'typename': typename},
                extra={'spider': spider},
            )
        return None

    def _log_download_errors(self, spider_failure: Failure, download_failure: Failure, request: Request,
                             spider: Spider) -> Union[Failure, None]:
        """Log and silence errors that come from the engine (typically download
        errors that got propagated thru here).

        spider_failure: the value passed into the errback of self.call_spider()
        download_failure: the value passed into _scrape2() from
        ExecutionEngine._handle_downloader_output() as "result"
        """
        if not download_failure.check(IgnoreRequest):
            if download_failure.frames:
                logkws = self.logformatter.download_error(download_failure, request, spider)
                logger.log(
                    *logformatter_adapter(logkws),
                    extra={'spider': spider},
                    exc_info=failure_to_exc_info(download_failure),
                )
            else:
                errmsg = download_failure.getErrorMessage()
                if errmsg:
                    logkws = self.logformatter.download_error(
                        download_failure, request, spider, errmsg)
                    logger.log(
                        *logformatter_adapter(logkws),
                        extra={'spider': spider},
                    )

        if spider_failure is not download_failure:
            return spider_failure
        return None

    def _itemproc_finished(self, output: Any, item: Any, response: Response, spider: Spider) -> None:
        """ItemProcessor finished for the given ``item`` and returned ``output``
        """
        assert self.slot is not None  # typing
        self.slot.itemproc_size -= 1
        if isinstance(output, Failure):
            ex = output.value
            if isinstance(ex, DropItem):
                logkws = self.logformatter.dropped(item, ex, response, spider)
                if logkws is not None:
                    logger.log(*logformatter_adapter(logkws), extra={'spider': spider})
                return self.signals.send_catch_log_deferred(
                    signal=signals.item_dropped, item=item, response=response,
                    spider=spider, exception=output.value)
            else:
                logkws = self.logformatter.item_error(item, ex, response, spider)
                logger.log(*logformatter_adapter(logkws), extra={'spider': spider},
                           exc_info=failure_to_exc_info(output))
                return self.signals.send_catch_log_deferred(
                    signal=signals.item_error, item=item, response=response,
                    spider=spider, failure=output)
        else:
            logkws = self.logformatter.scraped(output, response, spider)
            if logkws is not None:
                logger.log(*logformatter_adapter(logkws), extra={'spider': spider})
            return self.signals.send_catch_log_deferred(
                signal=signals.item_scraped, item=output, response=response,
                spider=spider)
