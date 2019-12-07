from databaseStructure import MyHashTable
from databaseStructure import MyBTreeTable


# Table for mapping user input name and actual table.
class ParameterAssignmentTable:
    def __init__(self):
        self.parameter_assignment_table = {}
        self.database_type = 'hash'

    # Set database type
    # type database_type: str
    # rtype None
    def set_database_type(self, database_type):
        self.database_type = database_type

    # Check if the key exist in the dict.
    # type key: str
    # rtype boolean
    def is_key_exist(self, key):
        return self.parameter_assignment_table.get(key)

    # Get the table by key
    # type key: str
    # rtype DbObject
    def get_parameter_assignment_table(self, key):
        return self.parameter_assignment_table[key]

    # Initialize the table by name.
    # type assign_name: str
    # type db_object: DbObject
    # rtype None
    def create_parameter_assignment_table(self, assign_name):
        if self.database_type == 'hash':
            self.parameter_assignment_table[assign_name] = MyHashTable.MyHashTable()
        else:
            self.parameter_assignment_table[assign_name] = MyBTreeTable.MyBTreeTable()

    # Insert value into the table by assign name.
    # type assign_name: str
    # type metadata: array
    # type main_table: dictionary
    # rtype None
    def insert_parameter_assignment_table(self, assign_name, metadata, data_type, main_table):
        target_table = self.parameter_assignment_table[assign_name]
        target_table.assign_metadata(metadata)
        target_table.assign_data_type(data_type)
        target_table.assign_main_table(main_table)
        target_table.assign_name(assign_name)
