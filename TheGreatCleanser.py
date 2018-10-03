import re
from html import unescape
import enchant
from stemmer import PorterStemmer


class TheGreatCleanser:
    """docstring for TheGreatCleanser"""
    ENDict = enchant.Dict('en_GB')
    DADict = enchant.Dict('da')

    stemmer = PorterStemmer()

    langLists = {
        'DA': set(
            ['af', 'alle', 'andet', 'andre', 'at', 'begge', 'da', 'de', 'den', 'denne', 'der', 'deres', 'det', 'dette',
             'dig', 'din', 'dog', 'du', 'ej', 'eller', 'en', 'end', 'ene', 'eneste', 'enhver', 'et', 'fem', 'fire',
             'flere', 'fleste', 'for', 'fordi', 'forrige', 'fra', 'få', 'før', 'god', 'han', 'hans', 'har', 'hendes',
             'her', 'hun', 'hvad', 'hvem', 'hver', 'hvilken', 'hvis', 'hvor', 'hvordan', 'hvorfor', 'hvornår', 'i',
             'ikke', 'ind', 'ingen', 'intet', 'jeg', 'jeres', 'kan', 'kom', 'kommer', 'lav', 'lidt', 'lille', 'man',
             'mand', 'mange', 'med', 'meget', 'men', 'mens', 'mere', 'mig', 'ned', 'ni', 'nogen', 'noget', 'ny', 'nyt',
             'nær', 'næste', 'næsten', 'og', 'op', 'otte', 'over', 'på', 'se', 'seks', 'ses', 'som', 'stor', 'store',
             'syv', 'ti', 'til', 'to', 'tre', 'ud', 'var']),
        'EN': set(['a', 'able', 'about', 'above', 'abst', 'accordance', 'according', 'accordingly', 'across', 'act',
                   'actually', 'added', 'adj', 'affected', 'affecting', 'affects', 'after', 'afterwards', 'again',
                   'against', 'ah', 'all', 'almost', 'alone', 'along', 'already', 'also', 'although', 'always', 'am',
                   'among', 'amongst', 'an', 'and', 'announce', 'another', 'any', 'anybody', 'anyhow', 'anymore',
                   'anyone', 'anything', 'anyway', 'anyways', 'anywhere', 'apparently', 'approximately', 'are', 'aren',
                   'arent', 'arise', 'around', 'as', 'aside', 'ask', 'asking', 'at', 'auth', 'available', 'away',
                   'awfully', 'b', 'back', 'be', 'became', 'because', 'become', 'becomes', 'becoming', 'been', 'before',
                   'beforehand', 'begin', 'beginning', 'beginnings', 'begins', 'behind', 'being', 'believe', 'below',
                   'beside', 'besides', 'between', 'beyond', 'biol', 'both', 'brief', 'briefly', 'but', 'by', 'c', 'ca',
                   'came', 'can', 'cannot', 'can\'t', 'cause', 'causes', 'certain', 'certainly', 'co', 'com', 'come',
                   'comes', 'contain', 'containing', 'contains', 'could', 'couldnt', 'd', 'date', 'did', 'didn\'t',
                   'different', 'do', 'does', 'doesn\'t', 'doing', 'done', 'don\'t', 'down', 'downwards', 'due',
                   'during', 'e', 'each', 'ed', 'edu', 'effect', 'eg', 'eight', 'eighty', 'either', 'else', 'elsewhere',
                   'end', 'ending', 'enough', 'especially', 'et', 'et-al', 'etc', 'even', 'ever', 'every', 'everybody',
                   'everyone', 'everything', 'everywhere', 'ex', 'except', 'f', 'far', 'few', 'ff', 'fifth', 'first',
                   'five', 'fix', 'followed', 'following', 'follows', 'for', 'former', 'formerly', 'forth', 'found',
                   'four', 'from', 'further', 'furthermore', 'g', 'gave', 'get', 'gets', 'getting', 'give', 'given',
                   'gives', 'giving', 'go', 'goes', 'gone', 'got', 'gotten', 'h', 'had', 'happens', 'hardly', 'has',
                   'hasn\'t', 'have', 'haven\'t', 'having', 'he', 'hed', 'hence', 'her', 'here', 'hereafter', 'hereby',
                   'herein', 'heres', 'hereupon', 'hers', 'herself', 'hes', 'hi', 'hid', 'him', 'himself', 'his',
                   'hither', 'home', 'how', 'howbeit', 'however', 'hundred', 'i', 'id', 'ie', 'if', 'i\'ll', 'im',
                   'immediate', 'immediately', 'importance', 'important', 'in', 'inc', 'indeed', 'index', 'information',
                   'instead', 'into', 'invention', 'inward', 'is', 'isn\'t', 'it', 'itd', 'it\'ll', 'its', 'itself',
                   'i\'ve', 'j', 'just', 'k', 'keep', 'keeps', 'kept', 'kg', 'km', 'know', 'known', 'knows', 'l',
                   'largely', 'last', 'lately', 'later', 'latter', 'latterly', 'least', 'less', 'lest', 'let', 'lets',
                   'like', 'liked', 'likely', 'line', 'little', '\'ll', 'look', 'looking', 'looks', 'ltd', 'm', 'made',
                   'mainly', 'make', 'makes', 'many', 'may', 'maybe', 'me', 'mean', 'means', 'meantime', 'meanwhile',
                   'merely', 'mg', 'might', 'million', 'miss', 'ml', 'more', 'moreover', 'most', 'mostly', 'mr', 'mrs',
                   'much', 'mug', 'must', 'my', 'myself', 'n', 'na', 'name', 'namely', 'nay', 'nd', 'near', 'nearly',
                   'necessarily', 'necessary', 'need', 'needs', 'neither', 'never', 'nevertheless', 'new', 'next',
                   'nine', 'ninety', 'no', 'nobody', 'non', 'none', 'nonetheless', 'noone', 'nor', 'normally', 'nos',
                   'not', 'noted', 'nothing', 'now', 'nowhere', 'o', 'obtain', 'obtained', 'obviously', 'of', 'off',
                   'often', 'oh', 'ok', 'okay', 'old', 'omitted', 'on', 'once', 'one', 'ones', 'only', 'onto', 'or',
                   'ord', 'other', 'others', 'otherwise', 'ought', 'our', 'ours', 'ourselves', 'out', 'outside', 'over',
                   'overall', 'owing', 'own', 'p', 'page', 'pages', 'part', 'particular', 'particularly', 'past', 'per',
                   'perhaps', 'placed', 'please', 'plus', 'poorly', 'possible', 'possibly', 'potentially', 'pp',
                   'predominantly', 'present', 'previously', 'primarily', 'probably', 'promptly', 'proud', 'provides',
                   'put', 'q', 'que', 'quickly', 'quite', 'qv', 'r', 'ran', 'rather', 'rd', 're', 'readily', 'really',
                   'recent', 'recently', 'ref', 'refs', 'regarding', 'regardless', 'regards', 'related', 'relatively',
                   'research', 'respectively', 'resulted', 'resulting', 'results', 'right', 'run', 's', 'said', 'same',
                   'saw', 'say', 'saying', 'says', 'sec', 'section', 'see', 'seeing', 'seem', 'seemed', 'seeming',
                   'seems', 'seen', 'self', 'selves', 'sent', 'seven', 'several', 'shall', 'she', 'shed', 'she\'ll',
                   'shes', 'should', 'shouldn\'t', 'show', 'showed', 'shown', 'showns', 'shows', 'significant',
                   'significantly', 'similar', 'similarly', 'since', 'six', 'slightly', 'so', 'some', 'somebody',
                   'somehow', 'someone', 'somethan', 'something', 'sometime', 'sometimes', 'somewhat', 'somewhere',
                   'soon', 'sorry', 'specifically', 'specified', 'specify', 'specifying', 'still', 'stop', 'strongly',
                   'sub', 'substantially', 'successfully', 'such', 'sufficiently', 'suggest', 'sup', 'sure', 't',
                   'take', 'taken', 'taking', 'tell', 'tends', 'th', 'than', 'thank', 'thanks', 'thanx', 'that',
                   'that\'ll', 'thats', 'that\'ve', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'thence',
                   'there', 'thereafter', 'thereby', 'thered', 'therefore', 'therein', 'there\'ll', 'thereof',
                   'therere', 'theres', 'thereto', 'thereupon', 'there\'ve', 'these', 'they', 'theyd', 'they\'ll',
                   'theyre', 'they\'ve', 'think', 'this', 'those', 'thou', 'though', 'thoughh', 'thousand', 'throug',
                   'through', 'throughout', 'thru', 'thus', 'til', 'tip', 'to', 'together', 'too', 'took', 'toward',
                   'towards', 'tried', 'tries', 'truly', 'try', 'trying', 'ts', 'twice', 'two', 'u', 'un', 'under',
                   'unfortunately', 'unless', 'unlike', 'unlikely', 'until', 'unto', 'up', 'upon', 'ups', 'us', 'use',
                   'used', 'useful', 'usefully', 'usefulness', 'uses', 'using', 'usually', 'v', 'value', 'various',
                   '\'ve', 'very', 'via', 'viz', 'vol', 'vols', 'vs', 'w', 'want', 'wants', 'was', 'wasnt', 'way', 'we',
                   'wed', 'welcome', 'we\'ll', 'went', 'were', 'werent', 'we\'ve', 'what', 'whatever', 'what\'ll',
                   'whats', 'when', 'whence', 'whenever', 'where', 'whereafter', 'whereas', 'whereby', 'wherein',
                   'wheres', 'whereupon', 'wherever', 'whether', 'which', 'while', 'whim', 'whither', 'who', 'whod',
                   'whoever', 'whole', 'who\'ll', 'whom', 'whomever', 'whos', 'whose', 'why', 'widely', 'willing',
                   'wish', 'with', 'within', 'without', 'wont', 'words', 'world', 'would', 'wouldnt', 'www', 'x', 'y',
                   'yes', 'yet', 'you', 'youd', 'you\'ll', 'your', 'youre', 'yours', 'yourself', 'yourselves',
                   'you\'ve', 'z', 'zero']),
    }


    @staticmethod
    def removeTagIncludingContent(stream, tag):
        return re.sub('<' + tag + '[^>]*>[^<]*<\/' + tag + '[^>]*>', ' ', stream)

    @staticmethod
    def removeWhitespace(stream):
        return re.sub('\s+', ' ', stream)

    @staticmethod
    def removeTags(stream):
        return re.sub('<[^>]*>', ' ', stream)
        # /^$/!{s/<[^>]*>//g;p;}

    @staticmethod
    def extractContentFromTag(doc, tag):
        m = re.search('<' + tag + '[^>]*>(.*(?=<\/' + tag + '))', doc)
        if m is not None and m.group is not None:
            return m.group(1)
        else:
            return ""

    @staticmethod
    def removeStupidSymbols(stream):
        stream = re.sub('&[^;]*;', lambda x: unescape(x.group(0)), stream)
        return re.sub('[^\w\d\-]', ' ', stream)
        return stream

    @staticmethod
    def whichGoodLang(tokenStream):
        ENwords = 0
        DAwords = 0
        for w in tokenStream:
            if w == '':
                continue
            if TheGreatCleanser.ENDict.check(w):
                ENwords += 1
            if TheGreatCleanser.DADict.check(w):
                DAwords += 1

        ENwordsProp = ENwords / len(tokenStream)
        DAwordsProp = DAwords / len(tokenStream)

        biggest = ('EN', ENwordsProp) if ENwordsProp > DAwordsProp else ('DA', DAwordsProp)

        if biggest[1] < 0.60:
            return 'NaL', 0

        return biggest

    @staticmethod
    def removeStopwordsByLang(tokenList, lang):
        tokenList = list(filter(lambda w: w not in TheGreatCleanser.langLists[lang], tokenList))
        return tokenList

    @staticmethod
    def tokenize(stream):
        return stream.strip().split(' ')

    @staticmethod
    def cleanse(doc):
        doc = doc.lower()
        doc = doc.replace('\n', ' ').replace('\r', ' ')
        title = TheGreatCleanser.extractContentFromTag(doc, 'title')
        doc = TheGreatCleanser.removeTagIncludingContent(doc, 'style')
        doc = TheGreatCleanser.removeTagIncludingContent(doc, 'script')
        doc = TheGreatCleanser.removeTagIncludingContent(doc, 'head')
        doc = title + doc
        doc = TheGreatCleanser.removeTags(doc)
        doc = TheGreatCleanser.removeStupidSymbols(doc)
        doc = TheGreatCleanser.removeWhitespace(doc)

        tokenList = TheGreatCleanser.tokenize(doc)
        tokenList = list(filter(lambda token: token != '', tokenList))

        if len(tokenList) == 0:
            return 'NaL', tokenList

        lang = TheGreatCleanser.whichGoodLang(tokenList)[0]

        if lang == 'NaL':
            return lang, tokenList

        tokenList = TheGreatCleanser.removeStopwordsByLang(tokenList, lang)

        if lang == 'EN':
            tokenList = list(map(TheGreatCleanser.stemmer.stem, tokenList))

        return lang, tokenList
