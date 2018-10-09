import signal
import time
from Crawler.HorseCrawler import HorseCrawler
from Crawler.DebugService import DebugService


def exit_gracefully(signum, frame):
    """ Handle stop signals """
    global running
    ds.add('DONE', 'JUST ONE MORE!!')
    running = False


def timeTaken(func):
    start = time.time()
    func()
    return time.time() - start


ds = DebugService(['DONE', 'DOWNLOAD', 'TIME'])
ds = DebugService()

running = True

signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)

seed = ['https://antonchristensen.net']
crawler = HorseCrawler(seed, ds)

n = 10
times = []
while running:
    crawlTime = timeTaken(crawler.crawlSingle)

    times.insert(0, crawlTime)
    times = times[:n]
    avg = sum(times) / len(times)

    ds.add('TIME', 'Last page took {:.2} seconds. Average across last {} pages: {:.2} ({:.2} pages per second)'.format(crawlTime, len(times), avg, 1 / avg))

