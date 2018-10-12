import json
import math
import psycopg2
import psycopg2.extras


class HorseIndexer:
    """docstring for HorseIndexer"""
    def __init__(self, debugService):
        self.debugService = debugService
        self.dbConn = self.getDatabaseConn()
        self.documentFreq = {}
        self.nDocs = -1

    @staticmethod
    def getDatabaseConn():
        return psycopg2.connect(dbname='db', user='user', password='pass', host='localhost')

    def run(self):
        self.deleteIndex()
        documents = self.getAllPages()
        self.buildDocumentFrequencyTable(documents)
        self.nDocs = len(documents)
        
        for (pageId, tokens) in documents:
            self.createIndex(pageId, tokens)

        self.insertVectorLengths()

    def insertVectorLengths(self):
        c = self.dbConn.cursor()
        c.execute("""
            UPDATE pages
            SET term_vec_len = idx.veclen
            FROM (
                SELECT page_id, |/sum(tf_idf*tf_idf) FROM index GROUP BY page_id
            ) AS idx(page_id, veclen)
            WHERE pages.id = idx.page_id;
        """)
        self.dbConn.commit()

    def buildDocumentFrequencyTable(self, docuements):
        for (_, tokens) in docuements:
            tokens = set(tokens)
            for token in tokens:
                self.documentFreq[token] = self.documentFreq.get(token, 0)+1

    def getAllPages(self):
        c = self.dbConn.cursor()
        c.execute('SELECT id, document FROM pages')
        return list(
                    map(
                        lambda r: (r[0], json.loads(r[1])), 
                        c.fetchall()
                    )
                )

    def getPageId(self, url):
        c = self.dbConn.cursor()
        c.execute('SELECT * FROM pages WHERE url = %s', (url,))
        results = c.fetchone()

        if results is None:
            return None

        return results[0]

    def deleteIndex(self):
        c = self.dbConn.cursor()
        c.execute('DELETE FROM index')
        c.execute('ALTER SEQUENCE index_id_seq RESTART WITH 1')
        self.dbConn.commit()

    def createIndex(self, page_id, tokens):
        counts = dict()
        for term in tokens:
            counts[term] = counts.get(term, 0) + 1

        params = counts.items()
        params = [(page_id, pair[0], pair[1], self.calculateScore(pair)) for pair in params]

        c = self.dbConn.cursor()
        psycopg2.extras.execute_values(c, 'INSERT INTO index (page_id, term, frequency, tf_idf) VALUES %s', params, page_size=1000)
        self.dbConn.commit()

    def calculateScore(self, term):
        tfp = self.calculateTermFrequencyPrime(term)
        idf = self.calculateInverseDocumentFrequency(term)

        return tfp * idf

    def calculateTermFrequencyPrime(self, term):
        tf = term[1]
        if tf > 0:
            return 1 + math.log10(tf)
        else:
            return 0

    def calculateInverseDocumentFrequency(self, term):
        df = self.getDocumentFrequency(term[0])
        N = self.getNumberOfDocuments()

        return math.log10(N / df)

    def getDocumentFrequency(self, term):
        return self.documentFreq[term]
    
    def getNumberOfDocuments(self):
        return self.nDocs
