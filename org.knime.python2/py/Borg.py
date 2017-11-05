# Singleton/BorgSingleton.py
# Alex Martelli's 'Borg' singleton implementation

class Borg(object):
    _shared_state = {}
    def __init__(self):
        self.__dict__ = self._shared_state