from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

class HorseQueue:
    def __init__(self, horsedb):
        self.db = horsedb

    def empty(self):
        c = self.dbconn.cursor()
        c.execute("SELECT COUNT(*) FROM q;")
        return c.fetchone()[0] == 0        

    def get(self):
        result = self.db.popQueue()
        print("[QRET] {} was selected from the queueu".format(result))
        return result

    def put(self, url):
        host = self.db.getHost(getHost)

        c = self.dbconn.cursor()
        c.execute("INSERT INTO q (url) VALUES (?)", (url,))
        self.dbconn.commit()
