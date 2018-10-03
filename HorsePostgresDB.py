"""
Abstractions over the use of the HorseCrawler database
"""

from urllib.parse import urlparse
import json
import time
import psycopg2
import psycopg2.extras


class HorsePostgresDB:
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
        return psycopg2.connect(dbname='db', user='postgres', password='root', host='antonchristensen.net')
        # return psycopg2.connect(dbname='db', user='postgres', password='root', host='172.0.0.1')

    def tableExists(self, tableName):
        c = self.dbconn.cursor()
        c.execute("SELECT * FROM information_schema.tables WHERE table_name=%s;", (tableName,))

        return len(c.fetchall()) > 0

    def dropTable(self, tableName):
        c = self.dbconn.cursor()
        c.execute("DROP TABLE "+tableName+";")
        self.dbconn.commit()

    def rebuildDatabase(self):
        tables = ['pages', 'hosts', 'q', 'linktable', 'revindex']
        for t in tables:
            if self.tableExists(t):
                self.dropTable(t)

        self.createDatabase()

    def databaseExists(self):
        tables = ['pages', 'hosts', 'q', 'linktable', 'revindex']
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
                host_id       INT       NOT NULL,
                url           VARCHAR   NOT NULL,
                document      TEXT      NOT NULL,
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
                host_id   INT       NOT NULL,
                url       VARCHAR   NOT NULL
            );
        ''')

        c.execute(''' 
            CREATE TABLE linktable (
                from_page_id  INT       NOT NULL,
                to_page_url   VARCHAR   NOT NULL,
                PRIMARY KEY (from_page_id, to_page_url)
            );
        ''')

        c.execute(''' 
            CREATE TABLE revindex (
                id        SERIAL    NOT NULL PRIMARY KEY UNIQUE,
                term      VARCHAR   NOT NULL,
                page_id   INT       NOT NULL
            );
        ''')

        self.dbconn.commit()

        c.close()

    def isPageInTheDB(self, url):
        c = self.dbconn.cursor()
        c.execute('SELECT COUNT(*) FROM pages WHERE url = %s', (url,))
        return c.fetchone()[0] > 0

    def isPageOld(self, url):
        c = self.dbconn.cursor()
        c.execute('SELECT last_visited FROM pages WHERE url = %s', (url,))
        last_visited = c.fetchone()[0]
        return last_visited < time.time() - 604800

    def getHost(self, url):
        parsedUrl = urlparse(url)

        c = self.dbconn.cursor()
        c.execute('SELECT * FROM hosts WHERE host = %s', (parsedUrl.netloc,))
        results = c.fetchall()

        if len(results) == 0:
            return None

        return results[0]

    def insertOrUpdateHost(self, url):
        parsedUrl = urlparse(url)
        now = time.time()

        c = self.dbconn.cursor()

        try:
            if self.getHost(url) is None:
                c.execute(
                    'INSERT INTO hosts (host, last_visited, disallow_list, disallow_list_updated) VALUES (%s, %s, %s, %s)',
                    (parsedUrl.netloc, now, "", 0))
            else:
                c.execute('UPDATE hosts SET last_visited = %s WHERE host = %s', (now, parsedUrl.netloc))

            self.dbconn.commit()
        except psycopg2.InternalError:
            pass

    def getPageId(self, url):
        c = self.dbconn.cursor()
        c.execute('SELECT * FROM pages WHERE url = %s', (url,))
        results = c.fetchall()

        if len(results) == 0:
            return None

        return results[0]

    def insertPage(self, url, doc, lang):
        now = time.time()
        hostId = self.getHost(url)[0]

        c = self.dbconn.cursor()
        c.execute('INSERT INTO pages (host_id, url, document, lang, last_visited) VALUES (%s, %s, %s, %s, %s) RETURNING id;',
                  (hostId, url, doc, lang, now,))
        self.dbconn.commit()

        return c.fetchone()[0]

    def updatePage(self, url, doc, lang):
        now = time.time()
        c = self.dbconn.cursor()
        c.execute('UPDATE pages SET last_visited = %s, document = %s, lang = %s WHERE url = %s', (now, doc, lang, url))
        self.dbconn.commit()

    def updateLinktable(self, from_page_id, links):
        c = self.dbconn.cursor()
        c.execute('DELETE FROM linktable WHERE from_page_id = %s', (from_page_id,))
        self.dbconn.commit()

        params = [(from_page_id, link) for link in links]

        psycopg2.extras.execute_values(c, 'INSERT INTO linktable (from_page_id, to_page_url) VALUES %s', params, page_size=1000)

        self.dbconn.commit()

    def updateRevindex(self, page_id, tokens):
        c = self.dbconn.cursor()
        c.execute('DELETE FROM revindex WHERE page_id = %s', (page_id,))
        self.dbconn.commit()

        params = [(page_id, token) for token in tokens]

        psycopg2.extras.execute_values(c, 'INSERT INTO revindex (page_id, term) VALUES %s', params, page_size=1000)

        self.dbconn.commit()

    def insertOrUpdatePage(self, url, doc, normalized_outbound_urls, lang, tokens):
        self.insertOrUpdateHost(url)
        pageId = self.getPageId(url)

        if pageId is None:
            pageId = self.insertPage(url, doc, lang)
        else:
            self.updatePage(url, doc, lang)

        self.updateLinktable(pageId, normalized_outbound_urls)
        self.updateRevindex(pageId, tokens)

    def isInQueue(self, url):
        c = self.dbconn.cursor()
        c.execute('SELECT COUNT(*) FROM q WHERE url = %s', (url,))
        results = c.fetchone()
        return results[0] > 0

    def robertRecordIsRecent(self, url):
        host = self.getHost(url)
        if host is None:
            self.insertOrUpdateHost(url)
            return False

        return host[4] > time.time() - 86600

    def getRobertRecord(self, url):
        return json.loads(self.getHost(url)[3])

    def updateRobertRecord(self, url, disallowedList):
        now = time.time()
        host = urlparse(url).netloc
        encodedList = json.dumps(disallowedList)

        c = self.dbconn.cursor()
        c.execute('UPDATE hosts SET disallow_list = %s, disallow_list_updated = %s WHERE host = %s',
                  (encodedList, now, host,))
        self.dbconn.commit()

    def popQueue(self):
        hostRestitutionTimeInSeconds = 10
        now = time.time()
        c = self.dbconn.cursor()

        while True:
            c.execute('''
                SELECT * FROM q 
                JOIN hosts AS h 
                    ON q.host_id = h.id 
                WHERE h.last_visited < %s 
                ORDER BY q.id 
                ASC 
                LIMIT 1;''', ((now - hostRestitutionTimeInSeconds),))
            row = c.fetchone()

            if row is None:
                self.debugService.add('WARNING',
                                      'We visited everything recently... Lets just visit something again and not care about being so fucking polite')
                c.execute('SELECT * FROM q ORDER BY id ASC LIMIT 1;')
                row = c.fetchone()

            c.execute('DELETE FROM q WHERE id = %s', (row[0],))
            self.dbconn.commit()

            if c.rowcount == 1:
                return row[2]

            self.debugService.add('WARNING', 'Item was already removed from queueue')

    def qSize(self):
        c = self.dbconn.cursor()
        c.execute('SELECT COUNT(*) FROM q;')
        return c.fetchone()[0]

    def enqueue(self, url):
        host = self.getHost(url)
        if host is None:
            self.insertOrUpdateHost(url)
            host = self.getHost(url)

        hostId = host[0]

        c = self.dbconn.cursor()
        c.execute('INSERT INTO q (url, host_id) VALUES (%s, %s)', (url, hostId))
        self.dbconn.commit()
