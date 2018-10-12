import psycopg2
import psycopg2.extras


class SearchDB:
    def __init__(self):
        self.dbconn = SearchDB.getDatabaseConn()
        self.cache = {}
        self.nDocs = -1
        self.nTerms = -1

    @staticmethod
    def getDatabaseConn():
        return psycopg2.connect(dbname='db', user='user', password='pass', host='localhost')

    def getListOfRelevantDocuments(self, term):
        if term not in self.cache:
            c = self.dbconn.cursor()
            c.execute('SELECT page_id, frequency FROM index WHERE term = (%s);', (term,))
            self.cache[term] = c.fetchall()

        return list(map(lambda result: result[0], self.cache[term]))

    def getTermFrequency(self, term, document):
        return next((x[1] for x in self.cache[term] if x[0] == document), 0)

    def getDocumentFrequency(self, term):
        return len(self.cache[term])

    def getNumberOfDocuments(self):
        if self.nDocs < 0:
            c = self.dbconn.cursor()
            c.execute('SELECT COUNT(*) FROM pages;')
            result = c.fetchone()

            if result is None:
                self.nDocs = 0
            else:
                self.nDocs = result[0]

        return self.nDocs

    def getVocabSize(self):
        if self.nTerms < 0:
            c = self.dbconn.cursor()
            c.execute('SELECT COUNT(DISTINCT term) FROM index;')
            result = c.fetchone()

            if result is None:
                self.nTerms = 0
            else:
                self.nTerms = result[0]

        return self.nTerms
