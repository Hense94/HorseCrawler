#!/usr/bin/env python3

import math

import numpy as np
from SearchDB import SearchDB
from TheGreatCleanser import TheGreatCleanser


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


search = HorseSearcher()
print(search.search('2 antonchristensen'))
