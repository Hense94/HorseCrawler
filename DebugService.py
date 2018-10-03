class DebugService:
    def __init__(self, typesAllowed=['ALL']):
        self.typesAllowed = typesAllowed

    def add(self, type, message):
        if type in self.typesAllowed or 'ALL' in self.typesAllowed:
            spacing = ''
            for i in range(12 - len(type)):
                spacing += ' '

            if(type in ['ERROR']): type = "\033[91m"+type+"\033[0m"
            if(type in ['WARNING']): type = "\033[93m"+type+"\033[0m"
            if(type in ['INFO']): type = "\033[94m"+type+"\033[0m"
            if(type in ['DOWNLOAD']): type = "\033[96m"+type+"\033[0m"
            print('[{}]{}{}'.format(type, spacing, message))
