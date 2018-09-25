from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

class HorseQueue:
    def __init__(self, horsedb):
        self.db = horsedb

    def empty(self):
        return self.db.qSize() == 0     

    def get(self):
        result = self.db.popQueue()
        print("[QRET] {} was selected from the queueu".format(result))
        return result

    def put(self, url):
        self.db.enqueue(url)
