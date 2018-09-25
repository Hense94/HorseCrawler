from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
import urllib.request
import re
from socket import timeout
from queue import Queue
from Robert import Robert
from HorseDB import HorseDB
from HorseQueue import HorseQueue



class HorseCrawler:
    def __init__(self, seedUrls):
        self.db = HorseDB()
        self.q = HorseQueue(self.db.dbconn)
        for url in seedUrls:
            self.addToQueue(url)

    
    @staticmethod
    def _apply(f, l):
        for x in l:
            f(x)

    @staticmethod
    def isResourceURL(url):
        return bool(re.search(r'\.(pdf|jpg|jpeg|gif|png)$', url))

    def shouldCrawl(self, url):  # TODO: Implement more
        # Are we allowed?
        r = Robert('HorseBot', url, self.db)
        if not r.canAccessPath(url):
            return False

        # Is it a resource
        if (self.isResourceURL(url)):
            print('[ISRS] {} is probably some stupid format which we shouldn\'t read'.format(url))
            return False

        if self.db.isInQueue(url):
            print('[INQU] {} is already in Q'.format(url))
            return False

        if self.db.isPageInTheDB(url):
            isOld = self.db.isPageOld(url)
            if not isOld: 
                print('[INDB] {} is already in the DB and crawled recently'.format(url))
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

    @staticmethod
    def retrievePage(url):
        print("[DWLD] {}".format(url))

        request = urllib.request.Request(url)

        try:
            file = urllib.request.urlopen(request, timeout=1)
        except HTTPError as e:
            print('[ERRO] The server couldn\'t fulfill the request.')
            print('[ERRO] Error code: ', e.code)
            return None
        except URLError as e:
            print('[ERRO] We failed to reach a server.')
            print('[ERRO] Reason: ', e.reason)
            return None
        except timeout as e:
            e.reason = "timeout"
            print('[ERRO] It took too long to reach {}.'.format(url))
            print('[ERRO] Reason: ', e.reason)
            return None
        else:
            return file

    def parsePage(self, url):
        doc = self.retrievePage(url)
        if doc is None:
            return

        try:
            html = doc.read().decode('utf-8')
        except UnicodeDecodeError as e:
            print('[ERRO] We failed to decode a symbol on {}... Lets throw out the entire thing then.'.format(url))
            html = ""

        self.db.insertOrUpdatePage(url, html)

        normalizedUrls = self.normalizeUrls(url, self.extractLinks(html))

        self._apply(self.addToQueue, list(filter(self.shouldCrawl, normalizedUrls)))

    def addToQueue(self, url):
        if self.shouldCrawl(url):
            self.q.put(url)

    def crawlSingle(self):
        if self.q.empty():
            print("[DONE] The web has been crawled. No more to see here")
            return
        url = self.q.get()
        self.parsePage(url)
        print()
