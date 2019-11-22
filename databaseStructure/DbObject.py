# DB object which store in the dictionary
class DbObject:
    def __init__(self, value):
        self.value = value
        self.is_deleted = False
