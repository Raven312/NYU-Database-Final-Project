from databaseStructure import DbObject
from generalFunction import GeneralFunction
from ConditionObject import ConditionObject
import time
import operator
import numpy as np


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

    # Sort this table by parameters in metadata.
    # type require_metadata: array - desired metadata which exist in this table
    # rtype require_metadata: array - return the input require_metadata
    # rtype new_dic: dictionary - new dictionary that copy from this table but only with required columns
    def sort(self, require_metadata):
        start = time.time()

        # get the index of parameters in metadata
        require_index = GeneralFunction.get_index_of_metadata(self.metadata, require_metadata)

        # print(list(self.main_table.values())[0].value[require_index[0]])
        # define prepare_sort function and import operator
        # import itemgetter to take input from list variables and sort

        sorted_table = sorted(self.main_table.items(), key=lambda x: self.prepare_sort(x, require_index))

        # covert the list of tuples into dictionary
        new_dict = GeneralFunction.convert_tuples_into_dic(sorted_table)

        GeneralFunction.print_time(start, time.time())
        return self.metadata, new_dict

        # get the sorted items list identifying whether it is integer or string, further return items.
    @staticmethod
    def prepare_sort(x, require_index):
        items = operator.itemgetter(*require_index)(x[1].value)
        item_list = list(items)
        for idx, value in enumerate(item_list):
            if value.isdigit():
                item_list[idx] = int(value)
        items = tuple(item_list)
        return items


    # Return the value of moving average in metadata.
    # type variables: array - first is header and second is moving average period of time n
    # rtype new_metadata: array - return the key and input header
    # rtype new_dic: dictionary - return the value of key and input header
    def mov_avg(self, variables):
        require_header = variables[0]
        period_of_time = int(variables[1])
        start = time.time()

        new_metadata = [self.metadata[0], require_header]
        # get the index of parameters in metadata
        require_index = GeneralFunction.get_index_of_metadata(self.metadata, [require_header])[0]

        new_dict = {}
        # Moving average counting
        # current_index as list(self.main_table.keys()).index(key)
        for key in self.main_table:
            current_index = list(self.main_table.keys()).index(key)
            moving_avg = 0
            count = 0
        # counting period_of_time and get new_dict[key]
            for j in range(0, period_of_time):
                index = current_index - j
                if index < 0:
                    break
                current_dbobj = list(self.main_table.values())[index]
                moving_avg += int(current_dbobj.value[require_index])
                count += 1

            new_dict[key] = moving_avg/count

        GeneralFunction.print_time(start, time.time())
        return new_metadata, new_dict

    # Return average value of the column.
    # type require_metadata: array - desired metadata which exist in this table
    # rtype require_metadata: array - return the input avg_ + require_metadata
    # rtype new_dic: dictionary - new dictionary that have key is none and value is the outcome average
    def average(self, require_metadata):
        start = time.time()

        # get the index of parameters in metadata
        require_index = GeneralFunction.get_index_of_metadata(self.metadata, require_metadata)

        total_sum = 0
        for key in self.main_table:
            total_sum += int(self.main_table[key].value[require_index[0]])
        total_average = total_sum / len(self.main_table)

        GeneralFunction.print_time(start, time.time())

        return ['avg_' + require_metadata[0]], {None: total_average}

    # Return sum of the sum_header and group by group_header.
    # type sum_header: str - the column to sum up
    # type group_header: array - the columns to group by
    # rtype array - return the group_header and 'sum_' + sum_header
    # rtype group_dict: dictionary - new dictionary which have key equal to the value of the column in group_header,
    # and value of group_header and sum of sum_header.
    def sum_group(self, sum_header, group_header):
        start = time.time()

        sum_index = GeneralFunction.get_index_of_metadata(self.metadata, [sum_header])
        # get the index of parameters in metadata
        group_index = GeneralFunction.get_index_of_metadata(self.metadata, group_header)

        group_dict = {}

        for key in self.main_table:
            new_key = []
            for index in group_index:
                new_key.append(self.main_table[key].value[index])

            new_key_string = str(new_key)
            if group_dict.get(new_key_string):
                value = group_dict[new_key_string]
                value[-1] = str(int(value[-1]) + int(self.main_table[key].value[sum_index[0]]))
                group_dict[new_key_string] = value
            else:
                group_dict[new_key_string] = [k for k in new_key] + [self.main_table[key].value[sum_index[0]]]

        GeneralFunction.print_time(start, time.time())

        return [header for header in group_header] + ['sum_' + sum_header], group_dict
