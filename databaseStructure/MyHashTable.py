from databaseStructure.DatabaseFunction import DatabaseFunction


# Table in Hash data structure.
class MyHashTable(DatabaseFunction):
    def __init__(self):
        super().__init__()
        # Implement Hash Table from python in-build dictionary
        self.name = ''
        self.data_type = []
        self.metadata = []
        self.main_table = {}
        self.index = {}
