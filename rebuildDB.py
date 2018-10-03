from DebugService import DebugService
from HorsePostgresDB import HorsePostgresDB

dbgservice = DebugService()
db = HorsePostgresDB(dbgservice, True)
