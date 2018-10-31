#!/usr/bin/env python3

import numpy as np
from SearchDB import SearchDB
from TheGreatCleanser import TheGreatCleanser
import sys


class HorseSearcher:
    def __init__(self):
        self.db = SearchDB()
        self.query = ''

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
        alpha = 0.99
        lang, queryTerms = self.cleanQuery(query)
        queryVector = [1 for _ in range(len(queryTerms))]
        result = {}
        documents = set()

        for term in queryTerms:
            for document in self.db.getListOfRelevantDocuments(term):
                documents.add(document)

        print(documents)

        for document in documents:
            result[document[0]] = []
            for term in queryTerms:
                result[document[0]].append(next((float(x[1]) for x in self.db.cache[term] if x[0] == document[0]), 0))

            result[document[0]] = self.angle_between(queryVector, result[document[0]])*alpha + document[1]*(1-alpha)

        print(result)
        result = [(d, s) for (d, s) in result.items()]
        
        # result = self.db.getRanks(result)
        # print(result)


        result.sort(key=lambda item: item[1], reverse=True)

        result = self.db.fillQueryResult(result)
        return result

    def angle_between(self, v1, v2_u):
        v1_u = v1 / np.linalg.norm(v1)
        return np.arccos(np.clip(np.dot(v1_u, v2_u), -1.0, 1.0))


search = HorseSearcher()
results = search.search(' '.join(sys.argv[1:]))
for res in results:
    print(res)
