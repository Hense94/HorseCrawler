"""
    HorseQueue used to be usefull...
    not so much anymore.
"""

class HorseQueue:
    """
        Nowadays the HorseQueue is wholy
        dependant on a HorseDB
    """
    def __init__(self, horse_db, debugService):
        """ Initializes a Q with a HorseDB instance"""
        self.debugService = debugService
        self.horse_db = horse_db

    def empty(self):
        """Checks if the Q is empty"""
        return self.horse_db.qSize() == 0

    def get(self):
        """Removes an element from the queue and returns it"""
        result = self.horse_db.popQueue()
        self.debugService.add('INFO', 'Got {} from the queue'.format(result))
        return result

    def put(self, url):
        """Inserts an element into the queue"""
        self.horse_db.enqueue(url)
