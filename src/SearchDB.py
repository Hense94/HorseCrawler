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
            c.execute('SELECT page_id, tf_idf/pages.term_vec_len, rank FROM index JOIN pages ON pages.id = index.page_id WHERE term = (%s);', (term,))
            self.cache[term] = c.fetchall()

        return list(map(lambda result: (result[0], float(result[2])), self.cache[term]))

    def fillQueryResult(self, searchResults):
        c = self.dbconn.cursor()
        c.execute('select id, url from pages WHERE id IN({});'.format((','.join(map(lambda q: str(q[0]), searchResults)))))
        fillerData = c.fetchall()

        filledSearchRessults = []
        for result in searchResults:
            for data in fillerData:
                if data[0] == result[0]:
                    filledSearchRessults.append((data[1], result[1]))
                    break

        return filledSearchRessults

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
