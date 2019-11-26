# Table for mapping user input name and actual table.
class ParameterAssignmentTable:
    def __init__(self):
        self.parameter_assignment_table = {}

    def is_key_exist(self, key):
        return self.parameter_assignment_table.get(key)

    def get_parameter_assignment_table(self, key):
        return self.parameter_assignment_table[key]

    def create_parameter_assignment_table(self, assign_name, db_object):
        self.parameter_assignment_table[assign_name] = db_object

    def insert_parameter_assignment_table(self, assign_name, metadata, main_table):
        self.parameter_assignment_table[assign_name].metadata = metadata
        self.parameter_assignment_table[assign_name].main_table = main_table
