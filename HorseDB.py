"""
Abstractions over the use of the HorseCrawler database
"""

from urllib.parse import urlparse
import json
import time
import sqlite3

class HorseDB:
    def __init__(self, debugService):
        self.debugService = debugService
        self.dbconn = self.getDatabaseConn()
        if (not self.databaseExists()):
            self.createDatabase()

    @staticmethod
    def getDatabaseConn():
        return sqlite3.connect('db.sqlite3')

    def databaseExists(self):
        c = self.dbconn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='q';")

        return len(c.fetchall()) > 0

    def createDatabase(self):
        self.debugService.add('INFO', 'Creating database')

        c = self.dbconn.cursor()
        c.execute(''' 
            CREATE TABLE `pages` (
                `id`    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                `host_id`   INTEGER NOT NULL,
                `url`   TEXT NOT NULL,
                `document`  BLOB NOT NULL,
                `last_visited`    NUMERIC NOT NULL
            );
        ''')
        c.execute(''' 
            CREATE TABLE `hosts` (
                `id`    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                `host`  TEXT NOT NULL UNIQUE,
                `last_visited`  INTEGER NOT NULL,
                `disallow_list` TEXT NOT NULL,
                `disallow_list_updated` INTEGER NOT NULL
            );
        ''')
        c.execute(''' 
            CREATE TABLE `q` (
                `id`        INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                `host_id`   INTEGER NOT NULL,
                `url`       TEXT NOT NULL
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
        urlparts = urlparse(url)
        c = self.dbconn.cursor()
        c.execute('SELECT * FROM hosts WHERE host = ?', (urlparts.netloc,))
        results = c.fetchall()
        if len(results) == 0:
            return None
        return results[0]

    def insertOrUpdateHost(self, url):
        urlparts = urlparse(url)
        now = time.time()
        if self.getHost(url) is None:
            c = self.dbconn.cursor()
            c.execute(
                'INSERT INTO hosts (host, last_visited, disallow_list, disallow_list_updated) VALUES (?, ?, ?, ?)',
                (urlparts.netloc, now, "", 0))
            self.dbconn.commit()
        else:
            c = self.dbconn.cursor()
            c.execute('UPDATE hosts SET last_visited = ? WHERE host = ?', (now, urlparts.netloc))
            self.dbconn.commit()
        pass

    def getPage(self, url):
        c = self.dbconn.cursor()
        c.execute('SELECT * FROM pages WHERE url = ?', (url,))
        results = c.fetchall()
        if len(results) == 0:
            return None
        return results[0]

    def insertPage(self, url, doc):
        now = time.time()
        hostid = self.getHost(url)[0]

        c = self.dbconn.cursor()
        c.execute('INSERT INTO pages (host_id, url, document, last_visited) VALUES (?, ?, ?, ?)',
                  (hostid, url, doc, now,))
        self.dbconn.commit()

    def updatePage(self, url):
        now = time.time()
        c = self.dbconn.cursor()
        c.execute('UPDATE pages SET last_visited = ? WHERE url = ?', (now, url))
        self.dbconn.commit()

    def insertOrUpdatePage(self, url, doc):
        self.insertOrUpdateHost(url)
        if self.getPage(url) is None:
            self.insertPage(url, doc)
        self.updatePage(url)

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
        hostRestitutionTimeInSeconds = 1
        now = time.time()
        c = self.dbconn.cursor()

        # !!! TODO: WARNING: STRANGER DANGER: pythonDB API will not make these two SQL queries into one transaction !!!
        c.execute('''
            SELECT * FROM q 
            JOIN hosts AS h 
                ON q.host_id = h.id 
            WHERE h.last_visited < ? 
            ORDER BY id 
            ASC LIMIT 1;''', ((now - hostRestitutionTimeInSeconds),)
        )
        row = c.fetchone()
        if row is None:
            self.debugService.add('WARNING', 'We visited everything recently... Lets just visit something again and not care about being so fucking polite')
            c.execute('''SELECT * FROM q ORDER BY id ASC LIMIT 1;''')
            row = c.fetchone()
        c.execute('DELETE FROM q WHERE id = ?', (row[0],))
        # !!! TODO: WARNING: STRANGER DANGER: pythonDB API will not make these two SQL queries into one transaction !!!

        return row[2]

    def qSize(self):
        c = self.dbconn.cursor()
        c.execute('SELECT COUNT(*) FROM q;')
        return c.fetchone()[0]

    def enqueue(self, url):
        host = self.getHost(url)
        hostid = host[0]

        c = self.dbconn.cursor()
        c.execute('INSERT INTO q (url, host_id) VALUES (?, ?)', (url, hostid))
        self.dbconn.commit()

