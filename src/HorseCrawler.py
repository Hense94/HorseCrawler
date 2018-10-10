"""
Crawler implementation
"""

from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from http.client  import RemoteDisconnected
import urllib.request
import re
from ssl import CertificateError
from socket import timeout
from Robert import Robert
# from HorseSqliteDB import HorseDB
from HorsePostgresDB import HorseDB
# from HorseMySQLDB import HorseDB
from HorseQueue import HorseQueue
from TheGreatCleanser import TheGreatCleanser


class HorseCrawler:
    """
    Crawler will crawl the entire web.
    One page at time or untill it's on every
    Robert.txt page there is.
    """
    def __init__(self, seedUrls, debugService):
        self.debugService = debugService

        self.horse_db = HorseDB(debugService)
        self.queue = HorseQueue(self.horse_db, debugService)
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
                    parsedUrls.append(
                        parsedUrl.scheme + '://' + pu.netloc + pu.path + ('?' + pu.query if pu.query else ''))

        return parsedUrls

    def crawlSingle(self):
        if self.queue.empty():
            self.debugService.add('DONE', 'The web has been crawled. No more to see here.')
            return

        url = self.queue.get()
        self.parsePage(url)

    def parsePage(self, url):
        # Are we allowed?
        r = Robert('HorseBot', url, self.horse_db, self.debugService)
        if not r.canAccessPath(url):
            self.debugService.add('WARNING', 'Not allowed at {}'.format(url))
            return

        # Make the request
        doc = self.retrievePage(url)
        if doc is None:
            return

        try:
            html = doc.read().decode('utf-8')
        except (UnicodeDecodeError, UnicodeError):
            self.debugService.add('WARNING', 'Failed to read {} (reason: encoding error)'.format(url))
            return
        except timeout:
            self.debugService.add('WARNING', 'Failed to read {} (reason: Timeout)'.format(url))
            return

        lang, tokenList = TheGreatCleanser.cleanse(html)
        if lang == "NaL":
            self.debugService.add("WARNING", "NaL: {}".format(url))
            return

        normalizedUrls = list(set(self.normalizeUrls(url, self.extractLinks(html))))
        self.debugService.add('DOWNLOAD', 'Adding {}'.format(url))

        self.horse_db.insertOrUpdatePage(url, normalizedUrls, lang, tokenList)
        self._apply(self.addToQueue, normalizedUrls)

    def addToQueue(self, url):
        if self.shouldAddToQueue(url):
            self.queue.put(url)

    def retrievePage(self, url):
        request = urllib.request.Request(url)

        try:
            file = urllib.request.urlopen(request, timeout=1)
            self.horse_db.updateHostVisitTime(url)
        except HTTPError as e:
            self.debugService.add('WARNING', 'Server couldn\'t fulfill the request for {} (code {})'.format(url, e.code))
            return None
        except URLError as e:
            self.debugService.add('WARNING', 'Failed to reach {} (reason: {})'.format(url, e.reason))
            return None
        except timeout:
            self.debugService.add('WARNING', 'Failed to reach {} (reason: timeout)'.format(url))
            return None
        except CertificateError:
            self.debugService.add('WARNING', 'Failed to read {} (reason: SSL error)'.format(url))
            return None
        except RemoteDisconnected:
            self.debugService.add('WARNING', 'Failed to read {} (reason: SSL error)'.format(url))
            return None
        except (UnicodeDecodeError, UnicodeError):
            self.debugService.add('WARNING', 'Failed to read {} (reason: encoding error)'.format(url))
            return None
        else:
            return file

    def shouldAddToQueue(self, url):  # TODO: Implement more
        # Is it a resource?
        if self.is_resource_url(url):
            self.debugService.add('WARNING', '{} is probably some stupid format which we shouldn\'t read'.format(url))
            return False

        # Is it already in the queue?
        if self.horse_db.isInQueue(url):
            self.debugService.add('INFO', '{} is already in the queue'.format(url))
            return False

        # Has it been crawled recently?
        if self.horse_db.hasUrlBeenCrawledRecently(url):
            self.debugService.add('INFO', '{} is already in the DB and crawled recently'.format(url))
            return False

        return True
