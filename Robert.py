import re
import urllib.request
from urllib.error import URLError, HTTPError
from urllib.parse import urlparse
from socket import timeout


class Robert:
    def __init__(self, name, url, horseDB):
        self.name = name
        self.db = horseDB

        self.disallowedPaths = []
        self.domain = ''
        self.robotString = ''

        self.__getDomain(url)
        self.__getRobotsString()

    def __getDomain(self, url):
        parsedUrl = urlparse(url)
        self.domain = '{}://{}/'.format(parsedUrl.scheme, parsedUrl.netloc)

    def __getRobotsString(self):
        url = '{}robots.txt'.format(self.domain)

        if(self.db.robertRecordIsRecent(url)):
            self.disallowedPaths = self.db.getRobertRecord(url)
        else:
            request = urllib.request.Request(url)
            try:
                file = urllib.request.urlopen(request, timeout=1)
                print('[ROBR] {} was read'.format(url))
                self.robotString = file.read().decode('utf-8')
            except HTTPError as e:
                print('[ERRO] The server couldn\'t fulfill the request for {}.'.format(url))
                print('[ERRO] Error code: ', e.code)
                if e.code == 403:
                    self.disallowedPaths.append('/')
            except URLError as e:
                print('[ERRO] We failed to reach a server for request {}.'.format(url))
                print('[ERRO] Reason: ', e.reason)
            except timeout as e:
                e.reason = "timeout"
                print('[ERRO] It took too long to reach {}.'.format(url))
                print('[ERRO] Reason: ', e.reason)
            if len(self.disallowedPaths) == 0:
                self.__retrieveDisallowedPaths()

            self.db.updateRobertRecord(url, self.disallowedPaths)

                

    def __retrieveDisallowedPaths (self):
        relevantUserAgent = False

        for line in self.robotString.splitlines():
            line = line.strip()

            if line.startswith('User-agent'):
                if line.endswith(self.name):
                    relevantUserAgent = True
                elif line == 'User-agent: *':
                    relevantUserAgent = True
                else:
                    relevantUserAgent = False

            match = re.search('^Disallow:\s*(\S*)$', line)
            if match != None and relevantUserAgent:
                self.disallowedPaths.append(match[1].replace('$', ''))

    def canAccessPath(self, path):
        actualPath = urlparse(path).path

        for disallowedPath in self.disallowedPaths:
            if '*' in disallowedPath:
                splitDisallowedPath = disallowedPath.split('*')
                if actualPath.startswith(splitDisallowedPath[0]) and actualPath.endswith(splitDisallowedPath[1]):
                    return False
            else:
                if actualPath.startswith(disallowedPath):
                    return False

        return True
