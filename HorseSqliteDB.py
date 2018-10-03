"""
Abstractions over the use of the HorseCrawler database
"""

from urllib.parse import urlparse
import json
import time
import sqlite3

class HorseSqliteDB:
    def __init__(self, debugService):
        self.debugService = debugService
        self.dbconn = self.getDatabaseConn()

        if not self.databaseExists():
            self.createDatabase()

    @staticmethod
    def getDatabaseConn():
        return sqlite3.connect('db.sqlite3')

    def tableExists(self, tableName):
        c = self.dbconn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (tableName,))

        return len(c.fetchall()) > 0
        

    def databaseExists(self):
        return self.tableExists('pages') and self.tableExists('hosts') and self.tableExists('q') and self.tableExists('linkTable') and self.tableExists('revIndex')

    def deleteDatabase(self):
        pass

    def createDatabase(self):
        self.debugService.add('INFO', 'Creating database')

        c = self.dbconn.cursor()
        c.execute(''' 
            CREATE TABLE pages (
                id            INTEGER       NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                host_id       INTEGER       NOT NULL,
                url           TEXT          NOT NULL,
                document      TEXT          NOT NULL,
                lang          TEXT          NOT NULL,
                last_visited  NUMERIC       NOT NULL
            );
        ''')
        c.execute(''' 
            CREATE TABLE hosts (
                id                      INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                host                    TEXT    NOT NULL UNIQUE,
                last_visited            NUMERIC NOT NULL,
                disallow_list           TEXT    NOT NULL,
                disallow_list_updated   NUMERIC NOT NULL
            );
        ''')
        c.execute(''' 
            CREATE TABLE q (
                id        INTEGER   NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                host_id   INTEGER   NOT NULL,
                url       TEXT      NOT NULL
            );
        ''')

        c.execute(''' 
            CREATE TABLE linkTable (
                from_page_id  INTEGER   NOT NULL,
                to_page_url   TEXT      NOT NULL,
                PRIMARY KEY (from_page_id, to_page_url)
            );
        ''')

        c.execute(''' 
            CREATE TABLE revIndex (
                id        INTEGER   NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                term      TEXT      NOT NULL,
                page_id   INTEGER   NOT NULL
            );
        ''')

        self.dbconn.commit()

        c.close()

    def isPageInTheDB(self, url):
        c = self.dbconn.cursor()
        c.execute('SELECT COUNT(*) FROM pages WHERE url = ?', (url,))
        return c.fetchone()[0] > 0

    def isPageOld(self, url):
        c = self.dbconn.cursor()
        c.execute('SELECT last_visited FROM pages WHERE url = ?', (url,))
        last_visited = c.fetchone()[0]
        return last_visited < time.time() - 604800

    def getHost(self, url):
        parsedUrl = urlparse(url)

        c = self.dbconn.cursor()
        c.execute('SELECT * FROM hosts WHERE host = ?', (parsedUrl.netloc,))
        results = c.fetchall()

        if len(results) == 0:
            return None

        return results[0]

    def insertOrUpdateHost(self, url):
        parsedUrl = urlparse(url)
        now = time.time()

        c = self.dbconn.cursor()

        c.execute('BEGIN;')

        try:
            if self.getHost(url) is None:
                c.execute(
                    'INSERT INTO hosts (host, last_visited, disallow_list, disallow_list_updated) VALUES (?, ?, ?, ?)',
                    (parsedUrl.netloc, now, "", 0))
            else:
                    c.execute('UPDATE hosts SET last_visited = ? WHERE host = ?', (now, parsedUrl.netloc))
                    self.dbconn.commit()
        except sqlite3.OperationalError as e:
            pass

    def getPage(self, url):
        c = self.dbconn.cursor()
        c.execute('SELECT * FROM pages WHERE url = ?', (url,))
        results = c.fetchall()
        if len(results) == 0:
            return None
        return results[0]

    def insertPage(self, url, doc, lang):
        now = time.time()
        hostId = self.getHost(url)[0]

        c = self.dbconn.cursor()
        c.execute('INSERT INTO pages (host_id, url, document, last_visited, lang) VALUES (?, ?, ?, ?, ?)',
                  (hostId, url, doc, now, lang))
        self.dbconn.commit()

    def updatePage(self, url, doc, lang):
        now = time.time()
        c = self.dbconn.cursor()
        c.execute('UPDATE pages SET last_visited = ?, document = ?, lang = ? WHERE url = ?', (now, doc, url, lang))
        self.dbconn.commit()

    def updateLinkTable(self, url, links):
        page = self.getPage(url)
        from_page_id = page[0]

        c = self.dbconn.cursor()
        c.execute('DELETE FROM linkTable WHERE from_page_id = ?', (from_page_id,))
        self.dbconn.commit()

        params = list(map(lambda link: (from_page_id, link), links))
        c.executemany('INSERT INTO linkTable VALUES (?, ?)', params)
        self.dbconn.commit()

    def updateRevIndex(self, url, tokens):
        page = self.getPage(url)
        page_id = page[0]

        c = self.dbconn.cursor()
        c.execute('DELETE FROM revIndex WHERE page_id = ?', (page_id,))
        self.dbconn.commit()
        params = list(map(lambda token: (page_id, token), tokens))
        c.executemany('INSERT INTO revIndex (page_id, term) VALUES (?, ?)', params)
        self.dbconn.commit()

    def insertOrUpdatePage(self, url, doc, normalized_outbound_urls, lang, tokens):
        self.insertOrUpdateHost(url)
        if self.getPage(url) is None:
            self.insertPage(url, doc, lang)
        else:
            self.updatePage(url, doc, lang)
        self.updateLinkTable(url, normalized_outbound_urls)
        self.updateRevIndex(url, tokens)

    def isInQueue(self, url):
        c = self.dbconn.cursor()
        c.execute('SELECT COUNT(*) FROM q WHERE url = ?', (url,))
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
        c.execute('UPDATE hosts SET disallow_list = ?, disallow_list_updated = ? WHERE host = ?',
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
                WHERE h.last_visited < ? 
                ORDER BY q.id 
                ASC 
                LIMIT 1;''', ((now - hostRestitutionTimeInSeconds),))
            row = c.fetchone()

            if row is None:
                self.debugService.add('WARNING',
                                      'We visited everything recently... Lets just visit something again and not care about being so fucking polite')
                c.execute('SELECT * FROM q ORDER BY id ASC LIMIT 1;')
                row = c.fetchone()


            c.execute('DELETE FROM q WHERE id = ?', (row[0],))
            self.dbconn.commit()
            
            if(c.rowcount == 1):
                return row[2]
                
            



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
        c.execute('INSERT INTO q (url, host_id) VALUES (?, ?)', (url, hostId))
        self.dbconn.commit()
