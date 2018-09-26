"""
HorseCrawler implementation
"""

from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
import urllib.request
import re
from socket import timeout
from Robert import Robert
from HorseDB import HorseDB
from HorseQueue import HorseQueue


class HorseCrawler:
    """
    HorseCrawler will crawl the entire web.
    One page at time or untill it's on every
    Robert.txt page there is.
    """
    def __init__(self, seedUrls, debugService):
        self.debugService = debugService

        self.horse_db = HorseDB(debugService)
        self.queue = HorseQueue(self.horse_db, debugService)
        print ("JKLJKSLADJAKLD")
        for url in seedUrls:
            self.addToQueue(url)

    @staticmethod
    def _apply(func, items):
        for item in items:
            func(item)

    @staticmethod
    def is_resource_url(url):
        """ """
        return bool(re.search(r'\.(pdf|jpg|jpeg|gif|png)$', url))

    def shouldCrawl(self, url):  # TODO: Implement more
        # Are we allowed?
        r = Robert('HorseBot', url, self.horse_db, self.debugService)
        if not r.canAccessPath(url):
            return False

        # Is it a resource
        if self.is_resource_url(url):
            self.debugService.add('INFO', '{} is probably some stupid format which we shouldn\'t read'.format(url))
            return False

        if self.horse_db.isInQueue(url):
            self.debugService.add('INFO', '{} is already in the queueu'.format(url))
            return False

        if self.horse_db.isPageInTheDB(url):
            isOld = self.horse_db.isPageOld(url)
            if not isOld: 
                self.debugService.add('INFO', '{} is already in the DB and crawled recently'.format(url))

            return isOld
        return True

    @staticmethod
    def extractLinks(document):
        return re.findall(r'<a[^>]*href="([^#][^"]*)"[^>]*>', document)

    @staticmethod
    def normalizeUrls(originalUrl, urls):  # TODO: Implement
        parsedUrl = urlparse(originalUrl)
        urlFirstPart = '{}://{}'.format(parsedUrl.scheme, parsedUrl.netloc)

        parsedUrls = []

        for u in urls:
            pu = urlparse(u)
            if pu.scheme in ['', 'http', 'https']:
                if '' == pu.netloc:
                    parsedUrls.append(urlFirstPart + pu.path + ('?' + pu.query if pu.query else ''))
                else:
                    parsedUrls.append(parsedUrl.scheme + '://' + pu.netloc + pu.path + ('?' + pu.query if pu.query else ''))

        return parsedUrls

    def retrievePage(self, url):
        request = urllib.request.Request(url)

        try:
            file = urllib.request.urlopen(request, timeout=1)
        except HTTPError as e:
            self.debugService.add('ERROR', 'Server couldn\'t fulfill the request for {} (code {})'.format(url, e.code))
            return None
        except URLError as e:
            self.debugService.add('ERROR', 'Failed to reach {} (reason: {})'.format(url, e.reason))
            return None
        except timeout:
            self.debugService.add('ERROR', 'Failed to reach {} (reason: timeout)'.format(url))
            return None
        else:
            return file

    def parsePage(self, url):
        doc = self.retrievePage(url)
        if doc is None:
            return

        try:
            html = doc.read().decode('utf-8')
        except UnicodeDecodeError:
            self.debugService.add('ERROR', 'We failed to decode a symbol on {}... Lets throw out the entire thing then.'.format(url))
            return

        self.debugService.add('DOWNLOAD', 'Adding {}'.format(url))
        self.horse_db.insertOrUpdatePage(url, html)
        normalizedUrls = self.normalizeUrls(url, self.extractLinks(html))
        self._apply(self.addToQueue, list(filter(self.shouldCrawl, normalizedUrls)))

    def addToQueue(self, url):
        if self.shouldCrawl(url):
            self.queue.put(url)

    def crawlSingle(self):
        if self.queue.empty():
            self.debugService.add('DONE', 'The web has been crawled. No more to see here.')
            return
        url = self.queue.get()
        self.parsePage(url)
