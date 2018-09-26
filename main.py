"""
This is just a quick initialisation
of a HorseCrawler instance
"""

import signal
import time
from HorseCrawler import HorseCrawler
from DebugService import DebugService

DEBUGSERVICE = DebugService()
DEBUGSERVICE = DebugService(['DONE', 'DOWNLOAD', 'TIME', 'ERROR'])

RUNNING = True
def exit_gracefully(signum, frame):
    """ Handle stop signals """
    global RUNNING
    DEBUGSERVICE.add('DONE', 'JUST ONE MORE!!')
    RUNNING = False

signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)

SEED = ['https://antonchristensen.net']
CRAWLER = HorseCrawler(SEED, DEBUGSERVICE)

TIMES = []
while RUNNING:
    TIMESTART = time.time()
    
    CRAWLER.crawlSingle()
    
    TIMETAKEN = time.time() - TIMESTART
    TIMES.append(TIMETAKEN)
    AVERAGE = sum(TIMES) / len(TIMES)
    DEBUGSERVICE.add('TIME', 'Last page took {:.2} seconds. Average is {:.2} ({:.2} pages per second)'.format(TIMETAKEN, AVERAGE, 1 / AVERAGE))
