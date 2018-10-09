from Crawler.DebugService import DebugService
from Crawler.HorsePostgresDB import HorseDB
# from HorseSqliteDB import HorseDB
# from HorseMySQLDB import HorseDB

dbgservice = DebugService()
db = HorseDB(dbgservice, True)
