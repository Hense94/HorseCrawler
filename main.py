import signal
import time
from HorseCrawler import HorseCrawler
from DebugService import DebugService

ds = DebugService(['ALL'])
ds = DebugService(['DONE', 'DOWNLOAD', 'TIME'])

running = True
def exit_gracefully(signum, frame):
    global running
    ds.add('DONE', 'JUST ONE MORE!!')
    running = False

signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)

seed = ['https://antonchristensen.net']
crawler = HorseCrawler(seed, ds)

times = []
while running:
    timeStart = time.time()
    crawler.crawlSingle()
    timeTaken = time.time() - timeStart
    times.append(timeTaken)
    average = sum(times) / len(times)
    ds.add('TIME', 'Last page took {:.2} seconds. Average is {:.2} ({:.2} pages per second)'.format(timeTaken, average, 1 / average))
