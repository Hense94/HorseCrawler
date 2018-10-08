import time
import psycopg2

#dbconn = psycopg2.connect(dbname='db', user='postgres', password='root', host='antonchristensen.net')
dbconn = psycopg2.connect(dbname='db', user='user', password='pass', host='localhost')

updateFreq = 4

n = 2
lastN = []

last = -1

while True:
    c = dbconn.cursor()
    c.execute('SELECT COUNT(*) FROM pages;')

    new = c.fetchone()[0]
    if last == -1:
        last = new

    lastN.insert(0, new - last)
    lastN = lastN[:n]

    if len(lastN) > 0:
        average = sum(lastN) / len(lastN)
        print(average / updateFreq)

    last = new

    time.sleep(updateFreq)
