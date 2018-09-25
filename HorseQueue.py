from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

class HorseQueue:
    def __init__(self, db, debugService):
        self.debugService = debugService
        self.db = db

    def empty(self):
        return self.db.qSize() == 0     

    def get(self):
        result = self.db.popQueue()
        self.debugService.add('INFO', 'Got {} from the queue'.format(result))

        return result

    def put(self, url):
        self.db.enqueue(url)
