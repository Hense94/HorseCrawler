class DebugService:
    def __init__(self, typesAllowed=['ALL']):
        self.typesAllowed = typesAllowed

    def add(self, type, message):
        if type in self.typesAllowed or 'ALL' in self.typesAllowed:
            spacing = ''
            for i in range(12 - len(type)):
                spacing += ' '

            print('[{}]{}{}'.format(type, spacing, message))
