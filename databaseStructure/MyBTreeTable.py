from BTrees.OOBTree import OOBTree
from DatabaseFunction import DatabaseFunction


# Table in BTree data structure.
class MyBTreeTable(DatabaseFunction):
    def __init__(self):
        super().__init__()
        # Implement Hash Table from python in-build dictionary.
        self.metadata = []
        self.main_table = OOBTree()
