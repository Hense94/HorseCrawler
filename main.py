from HorseCrawler import HorseCrawler

# seed = ['https://antonchristensen.net', 'https://people.cs.aau.dk', 'https://kurt.com', 'https://en.wikipedia.org/wiki/Shm-reduplication']
seed = ['https://antonchristensen.net']
# seed = ['http://aerep.dk']

crawler = HorseCrawler(seed)

nPages = 1000
while True:
    crawler.crawlSingle()
    nPages = nPages-1
    if(nPages < 0):
        input("Press Enter to go further")

