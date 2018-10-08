"""
Abstractions over the use of the HorseCrawler database
"""

from urllib.parse import urlparse
import json
import time
import mysql
import mysql.connector


class HorseDB:
    def __init__(self, debugService, dropDB=False):
        self.debugService = debugService
        self.dbconn = self.getDatabaseConn()

        if dropDB:
            self.rebuildDatabase()
        else:
            if not self.databaseExists():
                self.rebuildDatabase()

    @staticmethod
    def getDatabaseConn():
        return mysql.connector.connect(database='db', user='root', passwd='root', host='localhost', port=3306)
        return mysql.connector.connect(database='db', user='postgres', passwd='root', host='antonchristensen.net', port=3303)

    def tableExists(self, tableName):
        c = self.dbconn.cursor()
        c.execute("SELECT * FROM information_schema.tables WHERE table_name = '{}'".format(tableName))

        return len(c.fetchall()) > 0

    def dropTable(self, tableName):
        c = self.dbconn.cursor()
        c.execute("DROP TABLE IF EXISTS {}".format(tableName))
        self.dbconn.commit()

    def rebuildDatabase(self):
        tables = ['pages', 'hosts', 'q', 'links', 'reverse_index']
        for t in tables:
            self.dropTable(t)

        self.createDatabase()

    def databaseExists(self):
        tables = ['pages', 'hosts', 'q', 'links', 'reverse_index']
        for t in tables:
            if not self.tableExists(t):
                return False

        return True

    def createDatabase(self):
        self.debugService.add('INFO', 'Creating database')

        c = self.dbconn.cursor()
        c.execute(''' 
            CREATE TABLE pages (
                id            SERIAL        NOT NULL PRIMARY KEY  UNIQUE,
                url           VARCHAR(1024) NOT NULL,
                lang          CHAR(2)       NOT NULL,
                last_visited  DOUBLE        NOT NULL
            );
        ''')

        c.execute(''' 
            CREATE TABLE hosts (
                id                      SERIAL          NOT NULL PRIMARY KEY UNIQUE,
                host                    VARCHAR(255)    NOT NULL UNIQUE,
                last_visited            DOUBLE          NOT NULL,
                disallow_list           TEXT            NOT NULL,
                disallow_list_updated   DOUBLE          NOT NULL
            );
        ''')

        c.execute(''' 
            CREATE TABLE q (
                id        SERIAL        NOT NULL PRIMARY KEY UNIQUE,
                url       VARCHAR(1024) NOT NULL,
                host      VARCHAR(255)  NOT NULL
            );
        ''')

        c.execute(''' 
            CREATE TABLE links (
                id            SERIAL        PRIMARY KEY,
                from_page_id  INT           NOT NULL,
                to_page_url   VARCHAR(1024) NOT NULL
            );
        ''')

        c.execute(''' 
            CREATE TABLE reverse_index (
                id        SERIAL        NOT NULL PRIMARY KEY UNIQUE,
                term      VARCHAR(100)  NOT NULL,
                frequency INT           NOT NULL,
                page_id   INT           NOT NULL
            );
        ''')

        self.dbconn.commit()

        c.close()

    def getDisallowListIfValid(self, url):
        parsedUrl = urlparse(url)
        now = time.time()
        oneWeek = 60 * 60 * 24 * 7

        c = self.dbconn.cursor()
        c.execute("SELECT id, disallow_list, disallow_list_updated FROM hosts WHERE host = '{}'".format(parsedUrl.netloc))
        results = c.fetchone()

        if results is None:
            return None, None

        if results[2] > now - oneWeek:
            return id, json.loads(results[1])
        else:
            return id, None

    def insertHost(self, url):
        parsedUrl = urlparse(url)

        c = self.dbconn.cursor()

        try:
            c.execute("INSERT INTO hosts (host, last_visited, disallow_list, disallow_list_updated) VALUES ('{}', {}, '{}', {})".format(parsedUrl.netloc, 0, '', 0))

            self.dbconn.commit()
        except Exception:
            print('Does this ever happen?!?!?')
            pass

    def setDisallowList(self, url, disallowList):
        encodedList = json.dumps(disallowList)
        now = time.time()
        host = urlparse(url).netloc

        c = self.dbconn.cursor()
        c.execute("UPDATE hosts SET disallow_list = '{}', disallow_list_updated = {} WHERE host = '{}'".format(encodedList, now, host))
        self.dbconn.commit()

    def updateHostVisitTime(self, url):
        now = time.time()
        host = urlparse(url).netloc

        c = self.dbconn.cursor()
        c.execute("UPDATE hosts SET last_visited = {} WHERE host = '{}'".format(now, host))
        self.dbconn.commit()

    def hasUrlBeenCrawledRecently(self, url):
        c = self.dbconn.cursor()
        c.execute("SELECT last_visited FROM pages WHERE url = '{}'".format(url))

        last_visited = c.fetchone()

        if last_visited is None:
            return False
        else:
            return last_visited[0] < time.time() - 604800

    def insertOrUpdatePage(self, url, normalized_outbound_urls, lang, tokens):
        pageId = self.getPageId(url)

        if pageId is None:
            pageId = self.insertPage(url, lang)
        else:
            self.updatePage(pageId, lang)
            self.deleteLinks(pageId)
            self.deleteIndex(pageId)

        self.insertLinks(pageId, normalized_outbound_urls)
        self.insertIndex(pageId, tokens)

    def getPageId(self, url):
        c = self.dbconn.cursor()
        c.execute("SELECT * FROM pages WHERE url = '{}'".format(url))
        results = c.fetchone()

        if results is None:
            return None

        return results[0]

    def insertPage(self, url, lang):
        now = time.time()

        c = self.dbconn.cursor()
        c.execute("INSERT INTO pages (url, lang, last_visited) VALUES (%s, %s, %s);", (url, lang, now))
        self.dbconn.commit()

        return c.lastrowid

    def updatePage(self, page_id, lang):
        now = time.time()

        c = self.dbconn.cursor()
        c.execute("UPDATE pages SET last_visited = {}, lang = '{}' WHERE id = {}".format(now, lang, page_id))
        self.dbconn.commit()

    def deleteLinks(self, from_page_id):
        c = self.dbconn.cursor()
        c.execute("DELETE FROM links WHERE from_page_id = {}".format(from_page_id))
        self.dbconn.commit()

    def deleteIndex(self, page_id):
        c = self.dbconn.cursor()
        c.execute("DELETE FROM reverse_index WHERE page_id = {}".format(page_id,))
        self.dbconn.commit()

    def insertLinks(self, from_page_id, links):
        params = [(from_page_id, link) for link in links]

        c = self.dbconn.cursor()
        c.executemany("INSERT INTO links (from_page_id, to_page_url) VALUES (%s, %s)", params)
        self.dbconn.commit()

    def insertIndex(self, page_id, tokens):
        counts = dict()
        for term in tokens:
            counts[term] = counts.get(term, 0) + 1

        params = counts.items()
        params = [(page_id, pair[0], pair[1]) for pair in params]

        c = self.dbconn.cursor()
        c.executemany("INSERT INTO reverse_index (page_id, term, frequency) VALUES (%s, %s, %s)", params)
        self.dbconn.commit()

    def isInQueue(self, url):
        c = self.dbconn.cursor()
        c.execute("SELECT COUNT(*) FROM q WHERE url = '{}'".format(url))
        results = c.fetchone()
        return results[0] > 0

    def enqueue(self, url):
        host = urlparse(url).netloc

        c = self.dbconn.cursor()
        c.execute("INSERT INTO q (url, host) VALUES ('{}', '{}')".format(url, host))
        self.dbconn.commit()

    def dequeue(self):
        hostRestitutionTimeInSeconds = 10
        now = time.time()
        c = self.dbconn.cursor()

        while True:
            c.execute('''
                SELECT * FROM q 
                LEFT JOIN hosts AS h 
                    ON q.host = h.host 
                WHERE h.last_visited < {} 
                ORDER BY q.id 
                ASC
                LIMIT 1;'''.format((now - hostRestitutionTimeInSeconds),))
            row = c.fetchone()

            if row is None:
                if self.qSize() > 0:
                    self.debugService.add('QUEUE', 'We visited everything recently... Lets just visit something again and not care about being so fucking polite')
                    c.execute("SELECT * FROM q LEFT JOIN hosts AS h ON q.host = h.host ORDER BY last_visited ASC LIMIT 1;")
                    row = c.fetchone()
                else:
                    self.debugService.add('DONE', 'The web has been crawled. No more to see here.')

            c.execute("DELETE FROM q WHERE id = {}".format(row[0]))
            self.dbconn.commit()

            if c.rowcount == 1:
                return row[1]

            self.debugService.add('QUEUE', 'Item was already removed from queueue')

    def qSize(self):
        c = self.dbconn.cursor()
        c.execute("SELECT COUNT(*) FROM q;")
        return c.fetchone()[0]
