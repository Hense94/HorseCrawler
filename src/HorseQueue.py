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

        self.maxQSizeAge = 25
        self.qSizeAge = self.maxQSizeAge
        self.qSize = -1

        self.maxInternalSize = 100
        self.internalQueue = set()

    def empty(self):
        """Checks if the Q is empty"""
        return self.size() == 0

    def size(self):
        if self.qSizeAge >= self.maxQSizeAge:
            self.qSize = self.horse_db.qSize()
            self.qSizeAge = 0

            if self.qSize > 1000:
                self.maxQSizeAge = 500
                self.maxInternalSize = 2500

        self.qSizeAge += 1
        return self.qSize

    def get(self):
        """Removes an element from the queue and returns it"""
        result = self.horse_db.dequeue()
        self.debugService.add('QUEUE', 'Got {} from the queue'.format(result))
        return result

    def put(self, url):
        """Inserts an element into the queue"""
        if self.size() < 50:
            self.debugService.add('INFO', 'Queue is tiny, emptying internal queue')
            self.horse_db.enqueue(url)
            return

        self.internalQueue.add(url)

        if len(self.internalQueue) >= self.maxInternalSize:
            self.emptyInternalQueue()


    def emptyInternalQueue(self):
        self.debugService.add('INFO', 'Emptying internal queue')
        self.horse_db.massEnqueue(self.internalQueue)
        self.internalQueue = set()
