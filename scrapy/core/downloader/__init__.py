import random
from time import time
from datetime import datetime
from collections import deque

from twisted.internet import defer, task

from scrapy.utils.defer import mustbe_deferred
from scrapy.utils.httpobj import urlparse_cached
from scrapy.resolver import dnscache
from scrapy import signals
from scrapy.core.downloader.middleware import DownloaderMiddlewareManager
from scrapy.core.downloader.handlers import DownloadHandlers


class Slot:
    """Downloader slot"""

    """
    何为Downloader Slot？在下载中间件中，slot可以理解为一个"插槽"，它是由一个字典类型实例化而成，slots里面存储了各种各种各样的Slot对象，其中key是
    request对象的域名，value是Slot对象。Slot对象用来控制Request的下载请求，一般由这些属性组成：（这个域名下的）并发值、下载延迟、随机延迟等
    这些属性主要是为了控制一个域名的请求策略，可以针对这个域名避免流量过大而ban ip的情况
    
    要注意，跟引擎的slot有些区别，当本质都是理解为一个"插槽"，用于控制每个爬虫/请求的组合件
    """

    def __init__(self, concurrency, delay, randomize_delay):
        self.concurrency = concurrency
        self.delay = delay
        self.randomize_delay = randomize_delay

        self.active = set()
        self.queue = deque()
        self.transferring = set()
        self.lastseen = 0
        self.latercall = None

    def free_transfer_slots(self):
        return self.concurrency - len(self.transferring)

    def download_delay(self):
        if self.randomize_delay:
            return random.uniform(0.5 * self.delay, 1.5 * self.delay)
        return self.delay

    def close(self):
        if self.latercall and self.latercall.active():
            self.latercall.cancel()

    def __repr__(self):
        cls_name = self.__class__.__name__
        return (f"{cls_name}(concurrency={self.concurrency!r}, "
                f"delay={self.delay:.2f}, "
                f"randomize_delay={self.randomize_delay!r})")

    def __str__(self):
        return (
            f"<downloader.Slot concurrency={self.concurrency!r} "
            f"delay={self.delay:.2f} randomize_delay={self.randomize_delay!r} "
            f"len(active)={len(self.active)} len(queue)={len(self.queue)} "
            f"len(transferring)={len(self.transferring)} "
            f"lastseen={datetime.fromtimestamp(self.lastseen).isoformat()}>"
        )


def _get_concurrency_delay(concurrency, spider, settings):
    delay = settings.getfloat('DOWNLOAD_DELAY')
    if hasattr(spider, 'download_delay'):
        delay = spider.download_delay

    if hasattr(spider, 'max_concurrent_requests'):
        concurrency = spider.max_concurrent_requests

    return concurrency, delay


class Downloader:

    DOWNLOAD_SLOT = 'download_slot'

    def __init__(self, crawler):
        self.settings = crawler.settings
        self.signals = crawler.signals
        self.slots = {}
        self.active = set()
        self.handlers = DownloadHandlers(crawler)
        self.total_concurrency = self.settings.getint('CONCURRENT_REQUESTS')
        self.domain_concurrency = self.settings.getint('CONCURRENT_REQUESTS_PER_DOMAIN')
        self.ip_concurrency = self.settings.getint('CONCURRENT_REQUESTS_PER_IP')
        self.randomize_delay = self.settings.getbool('RANDOMIZE_DOWNLOAD_DELAY')
        self.middleware = DownloaderMiddlewareManager.from_crawler(crawler)
        self._slot_gc_loop = task.LoopingCall(self._slot_gc)
        self._slot_gc_loop.start(60)

    def fetch(self, request, spider):
        def _deactivate(response):
            self.active.remove(request)
            return response

        self.active.add(request)

        # 进入下载器前，先过一边下载中间件里面的处理逻辑
        dfd = self.middleware.download(self._enqueue_request, request, spider)
        return dfd.addBoth(_deactivate)

    def needs_backout(self):
        return len(self.active) >= self.total_concurrency

    def _get_slot(self, request, spider):
        key = self._get_slot_key(request, spider)  # 根据request对象中的URL域名来获取对应的slot key
        if key not in self.slots:
            conc = self.ip_concurrency if self.ip_concurrency else self.domain_concurrency
            conc, delay = _get_concurrency_delay(conc, spider, self.settings)
            self.slots[key] = Slot(conc, delay, self.randomize_delay)

        return key, self.slots[key]

    def _get_slot_key(self, request, spider):
        if self.DOWNLOAD_SLOT in request.meta:
            return request.meta[self.DOWNLOAD_SLOT]

        key = urlparse_cached(request).hostname or ''
        if self.ip_concurrency:
            key = dnscache.get(key, key)  # 去DNS缓存中获取对应的IP键

        return key

    def _enqueue_request(self, request, spider):
        key, slot = self._get_slot(request, spider)  # 获取request相对应的Slot对象
        request.meta[self.DOWNLOAD_SLOT] = key

        def _deactivate(response):
            slot.active.remove(request)
            return response

        slot.active.add(request)  # active属性中添加request
        self.signals.send_catch_log(signal=signals.request_reached_downloader,
                                    request=request,
                                    spider=spider)
        deferred = defer.Deferred().addBoth(_deactivate)
        slot.queue.append((request, deferred))  # 向slot的队列queue添加request和对应的deferred对象
        self._process_queue(spider, slot)
        return deferred

    def _process_queue(self, spider, slot):
        """从slot对象的队列queue中获取请求并下载"""
        from twisted.internet import reactor
        if slot.latercall and slot.latercall.active():
            return

        # Delay queue processing if a download_delay is configured
        now = time()
        delay = slot.download_delay()
        if delay:
            penalty = delay - now + slot.lastseen
            if penalty > 0:
                slot.latercall = reactor.callLater(penalty, self._process_queue, spider, slot)
                return

        # Process enqueued requests if there are free slots to transfer for this slot
        while slot.queue and slot.free_transfer_slots() > 0:
            slot.lastseen = now
            request, deferred = slot.queue.popleft()
            dfd = self._download(slot, request, spider)  # 开始下载，重点关注，从这里开始，就会由scrapy通过handler转由twisted去下载了
            dfd.chainDeferred(deferred)
            # prevent burst if inter-request delays were configured
            if delay:
                self._process_queue(spider, slot)
                break

    def _download(self, slot, request, spider):
        # The order is very important for the following deferreds. Do not change!

        # 1. Create the download deferred
        dfd = mustbe_deferred(self.handlers.download_request, request, spider)

        # 2. Notify response_downloaded listeners about the recent download
        # before querying queue for next request

        # 在查询队列以获取下一个请求之前，通知response_downloaded监听者
        def _downloaded(response):
            self.signals.send_catch_log(signal=signals.response_downloaded,
                                        response=response,
                                        request=request,
                                        spider=spider)
            return response
        dfd.addCallback(_downloaded)

        # 3. After response arrives, remove the request from transferring
        # state to free up the transferring slot so it can be used by the
        # following requests (perhaps those which came from the downloader
        # middleware itself)

        # 将request加到slot集合中
        slot.transferring.add(request)

        def finish_transferring(_):
            # 响应到达后，将请求从传输状态中删除以释放传输槽，以便后续请求使用它（可能来自下载器中间件本身的请求）
            slot.transferring.remove(request)
            self._process_queue(spider, slot)
            self.signals.send_catch_log(signal=signals.request_left_downloader,
                                        request=request,
                                        spider=spider)
            return _

        return dfd.addBoth(finish_transferring)

    def close(self):
        self._slot_gc_loop.stop()
        for slot in self.slots.values():
            slot.close()

    def _slot_gc(self, age=60):
        mintime = time() - age
        for key, slot in list(self.slots.items()):
            if not slot.active and slot.lastseen + slot.delay < mintime:
                self.slots.pop(key).close()
