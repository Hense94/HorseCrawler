"""
Abstractions over the use of the HorseCrawler database
"""

from urllib.parse import urlparse
import json
import time
import psycopg2
from psycopg2._psycopg import IntegrityError


class HorseDB:
    def __init__(self, debugService):
        self.debugService = debugService
        self.dbconn = self.getDatabaseConn()

        # self.dbconn.set_isolation_level(3)

        if not self.databaseExists():
            self.createDatabase()

    @staticmethod
    def getDatabaseConn():
        return psycopg2.connect(dbname='db', user='postgres', password='root', host='127.0.0.1')

    def databaseExists(self):
        c = self.dbconn.cursor()
        c.execute("SELECT * FROM information_schema.tables WHERE table_name='q';")

        return len(c.fetchall()) > 0

    def createDatabase(self):
        self.debugService.add('INFO', 'Creating database')

        c = self.dbconn.cursor()
        c.execute(''' 
            CREATE TABLE pages (
                id            SERIAL    NOT NULL PRIMARY KEY  UNIQUE,
                host_id       INT       NOT NULL,
                url           VARCHAR   NOT NULL,
                document      TEXT      NOT NULL,
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
            CREATE TABLE linkTable (
                from_page_id  INT       NOT NULL,
                to_page_url   VARCHAR   NOT NULL,
                PRIMARY KEY (from_page_id, to_page_url)
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

        c.execute('BEGIN;')

        if self.getHost(url) is None:
            c.execute(
                'INSERT INTO hosts (host, last_visited, disallow_list, disallow_list_updated) VALUES (%s, %s, %s, %s)',
                (parsedUrl.netloc, now, "", 0))
        else:
            c.execute('UPDATE hosts SET last_visited = %s WHERE host = %s', (now, parsedUrl.netloc))

        self.dbconn.commit()

    def getPage(self, url):
        c = self.dbconn.cursor()
        c.execute('SELECT * FROM pages WHERE url = %s', (url,))
        results = c.fetchall()
        if len(results) == 0:
            return None
        return results[0]

    def insertPage(self, url, doc):
        now = time.time()
        hostId = self.getHost(url)[0]

        c = self.dbconn.cursor()
        c.execute('INSERT INTO pages (host_id, url, document, last_visited) VALUES (%s, %s, %s, %s)',
                  (hostId, url, doc, now,))
        self.dbconn.commit()

    def updatePage(self, url, doc):
        now = time.time()
        c = self.dbconn.cursor()
        c.execute('UPDATE pages SET last_visited = %s, document = %s WHERE url = %s', (now, doc, url))
        self.dbconn.commit()

    def updateLinkTable(self, url, links):
        page = self.getPage(url)
        from_page_id = page[0]

        c = self.dbconn.cursor()
        c.execute('DELETE FROM linkTable WHERE from_page_id = %s', (from_page_id,))
        self.dbconn.commit()
        for link in links:
            c.execute('INSERT INTO linkTable VALUES (%s, %s)', (from_page_id, link))
            self.dbconn.commit()

    def insertOrUpdatePage(self, url, doc, normalized_outbound_urls):
        self.insertOrUpdateHost(url)
        if self.getPage(url) is None:
            self.insertPage(url, doc)
        else:
            self.updatePage(url, doc)
        self.updateLinkTable(url, normalized_outbound_urls)

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
            try:
                c.execute('BEGIN;')

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

                return row[2]

            except IntegrityError:
                print('Race condition!!!')

            time.sleep(0.1)



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
