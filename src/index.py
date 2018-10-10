#!/usr/bin/env python3

import signal
import time
from HorseIndexer import HorseIndexer
from DebugService import DebugService

ds = DebugService(['DONE', 'DOWNLOAD', 'TIME'])
ds = DebugService()

indexer = HorseIndexer(ds)
indexer.run()