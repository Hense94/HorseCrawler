import math

import numpy as np
from HorseSearcher.SearchDB import SearchDB
from Shared.TheGreatCleanser import TheGreatCleanser


class HorseSearcher:
    def __init__(self):
        self.db = SearchDB()
        self.query = 'ingen test'

    def cleanQuery(self, query):
        query = query.lower()
        query = query.replace('\n', ' ').replace('\r', ' ')
        query = TheGreatCleanser.removeStupidSymbols(query)
        query = TheGreatCleanser.removeWhitespace(query)

        tokenizedQuery = self.tokenizeQuery(query)

        lang, tokenizedQuery = self.findLang(tokenizedQuery)

        if lang == 'NaL':
            return 'NaL', tokenizedQuery

        tokenizedQuery = TheGreatCleanser.removeStopwordsByLang(tokenizedQuery, lang)
        tokenizedQuery = list(map(TheGreatCleanser.stemmer.stem, tokenizedQuery))

        return lang, tokenizedQuery

    def tokenizeQuery(self, query):
        tokenizedQuery = TheGreatCleanser.tokenize(query)
        tokenizedQuery = list(filter(lambda token: token != '', tokenizedQuery))

        return tokenizedQuery

    def findLang(self, tokenizedQuery):
        if len(tokenizedQuery) == 0:
            return 'NaL', []

        lang = TheGreatCleanser.whichGoodLang(tokenizedQuery)[0]

        return lang, tokenizedQuery

    def search(self, query):
        lang, queryTerms = self.cleanQuery(query)
        queryVector = [1 for _ in range(len(queryTerms))]
        result = {}
        documents = set()

        for term in queryTerms:
            for document in self.db.getListOfRelevantDocuments(term):
                documents.add(document)

        for document in documents:
            result[document] = []
            for term in queryTerms:
                result[document].append(self.calculateScore(term, document))

            result[document] = self.angle_between(queryVector, result[document])

        temp = [(d, s) for (d, s) in result.items()]
        temp.sort(key=lambda item: item[1], reverse=True)
        return temp

    def calculateScore(self, term, document):
        tfp = self.calculateTermFrequencyPrime(term, document)
        idf = self.calculateInverseDocumentFrequency(term)

        return tfp * idf

    def calculateTermFrequencyPrime(self, term, document):
        tf = self.db.getTermFrequency(term, document)
        if tf > 0:
            return 1 + math.log10(tf)
        else:
            return 0

    def calculateInverseDocumentFrequency(self, term):
        df = self.db.getDocumentFrequency(term)
        N = self.db.getNumberOfDocuments()

        return math.log10(N / df)

    def combineResults(self, old, new):
        if len(old) == 0:
            old = new


    def angle_between(self, v1, v2):
        v1_u = v1 / np.linalg.norm(v1)
        v2_u = v2 / np.linalg.norm(v2)
        return np.arccos(np.clip(np.dot(v1_u, v2_u), -1.0, 1.0))

search = HorseSearcher()
print(search.search('2 antonchristensen'))
