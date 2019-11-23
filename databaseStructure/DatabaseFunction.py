from databaseStructure import DbObject
from generalFunction import GeneralFunction
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