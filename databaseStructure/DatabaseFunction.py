from databaseStructure import DbObject
from generalFunction import GeneralFunction
from ConditionObject import ConditionObject
import time


class DatabaseFunction:
    def __init__(self):
        # Implement Hash Table from python in-build dictionary.
        self.metadata = []
        self.main_table = {}

    # Assign the meta_data to the table.
    # type metadata: array
    def create_table(self, metadata):
        self.metadata = metadata

    # Copy this table based on the input metadata.
    # type require_metadata: array - desired metadata which exist in this table
    # rtype require_metadata: array - return the input require_metadata
    # rtype new_dic: dictionary - new dictionary that copy from this table but only with required columns
    def project(self, require_metadata):
        start = time.time()

        require_index = GeneralFunction.get_index_of_metadata(self.metadata, require_metadata)
        new_dict = {}

        for key in self.main_table:
            current_value = self.main_table[key].value
            new_dict[key] = DbObject.DbObject([current_value[i] for i in require_index])

        GeneralFunction.print_time(start, time.time())
        return require_metadata, new_dict

    # Select this table based on the condition.
    # type parameter: str - conditions
    # rtype require_metadata: array - return the original metadata
    # rtype new_dic: dictionary - new dictionary that copy from this table but only with required columns
    def select(self, parameter):
        start = time.time()

        new_dict = {}
        conditions = GeneralFunction.transform_condition_to_object(parameter)
        for cond in conditions:
            for key in self.main_table:
                if key not in new_dict:
                    current = self.main_table[key]
                    if ConditionObject.check_condition(cond, current, self.metadata):
                        new_dict[key] = current

        GeneralFunction.print_time(start, time.time())
        return self.metadata, new_dict

    # Input data from file.
    # type file_name: array - file name without type which exist in the rowData directory
    # type create_metadata_flag: boolean - to put the first line in the file as the metadata of this table
    def input_from_file(self, file_name, create_metadata_flag):
        start = time.time()

        for name in file_name:
            file = open('rowData/' + name + '.txt')
            line = file.readline().rstrip().strip().replace(" ", "")
            # Create metadata
            if create_metadata_flag:
                metadata_array = line.split('|')
                self.create_table(metadata_array)
                line = file.readline().rstrip().strip().replace(" ", "")
            # Input data into dictionary
            while line:
                line_item = line.split('|')
                self.main_table[line_item[0]] = DbObject.DbObject([line_item[i] for i in range(0, len(line_item))])
                line = file.readline().rstrip().strip().replace(" ", "")
            file.close()

        GeneralFunction.print_time(start, time.time())

    # Sort ths table by parameters in metadata.
    # type require_metadata: array - desired metadata which exist in this table
    # rtype require_metadata: array - return the input require_metadata
    # rtype new_dic: dictionary - new dictionary that copy from this table but only with required columns
    def sort(self, require_metadata):
        start = time.time()

        # get the index of parameters in metadata
        require_index = GeneralFunction.get_index_of_metadata(self.metadata, require_metadata)

        # print(list(self.main_table.values())[0].value[require_index[0]])
        if list(self.main_table.values())[0].value[require_index[0]].isdigit():
            sorted_table = sorted(self.main_table.items(), key=lambda x: int(x[1].value[require_index[0]]))
        else:
            sorted_table = sorted(self.main_table.items(), key=lambda x: x[1].value[require_index[0]])

        # covert the list of tuples into dictionary
        new_dict = GeneralFunction.convert_tuples_into_dic(sorted_table)

        GeneralFunction.print_time(start, time.time())
        return self.metadata, new_dict