from DebugService import DebugService
from HorsePostgresDB import HorseDB
# from HorseSqliteDB import HorseDB
# from HorseMySQLDB import HorseDB

dbgservice = DebugService()
db = HorseDB(dbgservice, True)
