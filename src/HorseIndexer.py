import json
import math
import psycopg2
import psycopg2.extras
import numpy as np


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

        self.calculatePageRank()

    def pagerank(self, M, eps=1.0e-8, d=0.85):
        N = M.shape[1]
        v = np.random.rand(N, 1)
        v = v / np.linalg.norm(v, 1)
        last_v = np.ones((N, 1), dtype=np.float32) * 100
        M_hat = (d * M) + (((1 - d) / N) * np.ones((N, N), dtype=np.float32))
        
        while np.linalg.norm(v - last_v, 2) > eps:
            last_v = v
            v = np.matmul(M_hat, v)
        return v

    def getLinkData(self):
        c = self.dbConn.cursor()
            # SELECT (from_page_id - 1) as from_page_id, (pages.id - 1) as to_page_id
            # FROM links LEFT JOIN pages ON to_page_url = url;
        c.execute('''
            SELECT (from_page_id - 1) as from_page_index, CASE WHEN id IS NULL THEN -1 ELSE id -1 END AS to_page_index
            FROM links LEFT JOIN pages ON to_page_url = url;
        ''')

        # links [
        #   1: 17
        #   1: 18
        #   2: 17
        # ]

        # _links {
        #   1: [17,18]
        # }
        temp = c.fetchall()
        links = {}
        for x in temp:
            links[x[0]] = links.get(x[0], [])
            links[x[0]].append(x[1])

        c.execute('''
            SELECT (from_page_id - 1) as from_page_id, count(*)
            FROM links
            GROUP BY from_page_id
            ORDER BY from_page_id ASC;
        ''')
        temp = c.fetchall()
        outcount = {}
        for x in temp:
            outcount[x[0]] = x[1]


        # TODO: fix outcount array

        return (links, outcount)


    def storeRideRanke(self, ranks):
        c = self.dbConn.cursor()
        psycopg2.extras.execute_values(c, 'UPDATE pages AS p SET rank = c.rank FROM (VALUES %s) AS c(id, rank) WHERE c.id = p.id', ranks, page_size=1000)
        self.dbConn.commit()

    def calculatePageRank(self):
        (links, outcount) = self.getLinkData()
        pageLinkGraph = np.zeros((self.nDocs+1, self.nDocs+1), dtype=np.float32)
        for from_ in range(0, self.nDocs):
            count = outcount.get(from_, 0)
            if count == 0: 
                continue
            outprop = 1/count
            remainder = 1
            for to_ in links.get(from_, []) :
                if to_ == -1: 
                    continue
                pageLinkGraph[to_,from_] = outprop
                remainder-=outprop
                pass
            pageLinkGraph[-1,from_] = remainder

        ranks = self.pagerank(pageLinkGraph, 0.001, 0.85)
        ranks = ranks.flatten()[:-1]
        rideranks = []
        for i in range(0,self.nDocs):
             rideranks.append((i+1, ranks[i]))
        self.storeRideRanke(rideranks)

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
