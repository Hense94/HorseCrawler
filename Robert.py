import re
from ssl import CertificateError
import urllib.request
from urllib.error import URLError, HTTPError
from urllib.parse import urlparse
from socket import timeout


class Robert:
    def __init__(self, name, url, db, debugService):
        self.debugService = debugService
        self.name = name
        self.db = db

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

        if self.db.robertRecordIsRecent(url):
            self.disallowedPaths = self.db.getRobertRecord(url)
        else:
            request = urllib.request.Request(url)
            try:
                file = urllib.request.urlopen(request, timeout=1)
                self.debugService.add('ROBERT', '{} was read'.format(url))
                self.robotString = file.read().decode('utf-8')
                self.__retrieveDisallowedPaths()

            except HTTPError as e:
                if e.code == 403:
                    self.disallowedPaths.append('/')
                    self.debugService.add('INFO', 'Not allowed to access {} '.format(url))
                else:
                    self.debugService.add('ERROR', 'Server couldn\'t fulfill the request for {} (code {})'.format(url, e.code))
            except URLError as e:
                self.debugService.add('ERROR', 'Failed to reach {} (reason: {})'.format(url, e.reason))
            except timeout:
                self.debugService.add('ERROR', 'Failed to reach {} (reason: timeout)'.format(url))
            except (UnicodeDecodeError, UnicodeError):
                self.debugService.add('ERROR', 'Failed to read {} (reason: encoding error)'.format(url))
            except CertificateError:
                self.debugService.add('ERROR', 'Failed to read {} (reason: SSL error)'.format(url))

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
