"""
Abstractions over the use of the Crawler database
"""

from urllib.parse import urlparse
import json
import time
import psycopg2
import psycopg2.extras


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
        return psycopg2.connect(dbname='db', user='user', password='pass', host='localhost')
        return psycopg2.connect(dbname='db', user='postgres', password='root', host='antonchristensen.net')

    def tableExists(self, tableName):
        c = self.dbconn.cursor()
        c.execute("SELECT * FROM information_schema.tables WHERE table_name=%s;", (tableName,))

        return len(c.fetchall()) > 0

    def dropTable(self, tableName):
        c = self.dbconn.cursor()
        c.execute("DROP TABLE " + tableName + ";")
        self.dbconn.commit()

    def rebuildDatabase(self):
        tables = ['pages', 'hosts', 'q', 'links', 'index']
        for t in tables:
            if self.tableExists(t):
                self.dropTable(t)

        self.createDatabase()

    def databaseExists(self):
        tables = ['pages', 'hosts', 'q', 'links', 'index']
        for t in tables:
            if not self.tableExists(t):
                return False

        return True

    def createDatabase(self):
        self.debugService.add('INFO', 'Creating database')

        c = self.dbconn.cursor()
        c.execute(''' 
            CREATE TABLE pages (
                id            SERIAL    NOT NULL PRIMARY KEY  UNIQUE,
                url           VARCHAR   NOT NULL,
                lang          VARCHAR(2) NOT NULL,
                last_visited  NUMERIC   NOT NULL
            );
        ''')

        c.execute(''' 
            CREATE TABLE hosts (
                id                      SERIAL  NOT NULL PRIMARY KEY UNIQUE,
                host                    VARCHAR NOT NULL UNIQUE,
                last_visited            NUMERIC NOT NULL,
                disallow_list           VARCHAR NOT NULL,
                disallow_list_updated   NUMERIC NOT NULL
            );
        ''')

        c.execute(''' 
            CREATE TABLE q (
                id        SERIAL    NOT NULL PRIMARY KEY UNIQUE,
                url       VARCHAR   NOT NULL,
                host      VARCHAR   NOT NULL
            );
        ''')

        c.execute(''' 
            CREATE TABLE links (
                from_page_id  INT       NOT NULL,
                to_page_url   VARCHAR   NOT NULL,
                PRIMARY KEY (from_page_id, to_page_url)
            );
        ''')

        c.execute(''' 
            CREATE TABLE index (
                id        SERIAL    NOT NULL PRIMARY KEY UNIQUE,
                term      VARCHAR   NOT NULL,
                frequency INT       NOT NULL,
                page_id   INT       NOT NULL
            );
        ''')

        self.dbconn.commit()

        c.close()

    def getDisallowListIfValid(self, url):
        parsedUrl = urlparse(url)
        now = time.time()
        oneWeek = 60 * 60 * 24 * 7

        c = self.dbconn.cursor()
        c.execute('SELECT id, disallow_list, disallow_list_updated FROM hosts WHERE host = %s', (parsedUrl.netloc,))
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
            c.execute('INSERT INTO hosts (host, last_visited, disallow_list, disallow_list_updated) VALUES (%s, %s, %s, %s)', (parsedUrl.netloc, 0, '', 0))

            self.dbconn.commit()
        except psycopg2.InternalError:
            print('Does this ever happen?!?!?')
            pass

    def setDisallowList(self, url, disallowList):
        encodedList = json.dumps(disallowList)
        now = time.time()
        host = urlparse(url).netloc

        c = self.dbconn.cursor()
        c.execute('UPDATE hosts SET disallow_list = %s, disallow_list_updated = %s WHERE host = %s', (encodedList, now, host,))
        self.dbconn.commit()

    def updateHostVisitTime(self, url):
        now = time.time()
        host = urlparse(url).netloc

        c = self.dbconn.cursor()
        c.execute('UPDATE hosts SET last_visited = %s WHERE host = %s', (now, host,))
        self.dbconn.commit()

    def hasUrlBeenCrawledRecently(self, url):
        c = self.dbconn.cursor()
        c.execute('SELECT last_visited FROM pages WHERE url = %s', (url,))

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
        c.execute('SELECT * FROM pages WHERE url = %s', (url,))
        results = c.fetchone()

        if results is None:
            return None

        return results[0]

    def insertPage(self, url, lang):
        now = time.time()

        c = self.dbconn.cursor()
        c.execute('INSERT INTO pages (url, lang, last_visited) VALUES (%s, %s, %s) RETURNING id;', (url, lang, now,))
        self.dbconn.commit()

        return c.fetchone()[0]

    def updatePage(self, page_id, lang):
        now = time.time()

        c = self.dbconn.cursor()
        c.execute('UPDATE pages SET last_visited = %s, lang = %s WHERE id = %s', (now, lang, page_id))
        self.dbconn.commit()

    def deleteLinks(self, from_page_id):
        c = self.dbconn.cursor()
        c.execute('DELETE FROM links WHERE from_page_id = %s', (from_page_id,))
        self.dbconn.commit()

    def deleteIndex(self, page_id):
        c = self.dbconn.cursor()
        c.execute('DELETE FROM index WHERE page_id = %s', (page_id,))
        self.dbconn.commit()

    def insertLinks(self, from_page_id, links):
        params = [(from_page_id, link) for link in links]

        c = self.dbconn.cursor()
        psycopg2.extras.execute_values(c, 'INSERT INTO links (from_page_id, to_page_url) VALUES %s', params, page_size=1000)
        self.dbconn.commit()

    def insertIndex(self, page_id, tokens):
        counts = dict()
        for term in tokens:
            counts[term] = counts.get(term, 0) + 1

        params = counts.items()
        params = [(page_id, pair[0], pair[1]) for pair in params]

        c = self.dbconn.cursor()
        psycopg2.extras.execute_values(c, 'INSERT INTO index (page_id, term, frequency) VALUES %s', params, page_size=1000)
        self.dbconn.commit()

    def isInQueue(self, url):
        c = self.dbconn.cursor()
        c.execute('SELECT COUNT(*) FROM q WHERE url = %s', (url,))
        results = c.fetchone()
        return results[0] > 0

    def enqueue(self, url):
        host = urlparse(url).netloc

        c = self.dbconn.cursor()
        c.execute('INSERT INTO q (url, host) VALUES (%s, %s)', (url, host))
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
                WHERE h.last_visited < %s 
                ORDER BY q.id 
                ASC NULLS FIRST
                LIMIT 1;''', ((now - hostRestitutionTimeInSeconds),))
            row = c.fetchone()

            if row is None:
                if self.qSize() > 0:
                    self.debugService.add('QUEUE', 'We visited everything recently... Lets just visit something again and not care about being so fucking polite')
                    c.execute('SELECT * FROM q LEFT JOIN hosts AS h ON q.host = h.host ORDER BY last_visited ASC NULLS FIRST LIMIT 1;')
                    row = c.fetchone()
                else:
                    self.debugService.add('DONE', 'The web has been crawled. No more to see here.')

            c.execute('DELETE FROM q WHERE id = %s', (row[0],))
            self.dbconn.commit()

            if c.rowcount == 1:
                return row[1]

            self.debugService.add('QUEUE', 'Item was already removed from queueue')

    def qSize(self):
        c = self.dbconn.cursor()
        c.execute('SELECT COUNT(*) FROM q;')
        return c.fetchone()[0]

