from DatabaseFunction import DatabaseFunction


# Table in Hash data structure.
class MyHashTable(DatabaseFunction):
    def __init__(self):
        super().__init__()
        # Implement Hash Table from python in-build dictionary
        self.meta_data = []
        self.main_hash_dict = {}
