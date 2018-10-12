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
                result[document].append(next((float(x[1]) for x in self.db.cache[term] if x[0] == document), 0))

            result[document] = self.angle_between(queryVector, result[document])

        temp = [(d, s) for (d, s) in result.items()]
        temp.sort(key=lambda item: item[1], reverse=True)

        temp = self.db.fillQueryResult(temp)
        return temp

    def angle_between(self, v1, v2_u):
        v1_u = v1 / np.linalg.norm(v1)
        return np.arccos(np.clip(np.dot(v1_u, v2_u), -1.0, 1.0))


search = HorseSearcher()
results = search.search(' '.join(sys.argv[1:]))
for res in results:
    print(res)
