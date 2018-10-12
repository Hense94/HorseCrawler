#!/usr/bin/env python3

import time
from HorseIndexer import HorseIndexer
from DebugService import DebugService

def timeTaken(func):
    start = time.time()
    func()
    return time.time() - start

ds = DebugService(['DONE', 'DOWNLOAD', 'TIME'])
ds = DebugService()

indexer = HorseIndexer(ds)
print(timeTaken(indexer.run))
