from databaseStructure import DbObject
from GeneralFunction import GeneralFunction
from ConditionObject import ConditionObject
import operator


class DatabaseFunction:
    def __init__(self):
        # Implement Hash Table from python in-build dictionary.
        self.name = ''
        self.data_type = []
        self.metadata = []
        self.main_table = {}
        self.index = {}

    # Assign the name to the table.
    # type assign_name: array
    def assign_name(self, assign_name):
        self.name = assign_name

    # Assign the meta_data to the table.
    # type metadata: array
    def assign_metadata(self, metadata):
        self.metadata = metadata

    def assign_data_type(self, data_type):
        self.data_type = data_type

    # Assign the main_table to the table.
    # type main_table: dict
    def assign_main_table(self, main_table):
        self.main_table = main_table

    # Copy this table based on the input metadata.
    # type require_metadata: array - desired metadata which exist in this table
    # rtype require_metadata: array - return the input require_metadata
    # rtype new_dic: dictionary - new dictionary that copy from this table but only with required columns
    def project(self, require_metadata):

        require_index = GeneralFunction.get_index_of_metadata(self.metadata, require_metadata)

        # Project data type
        data_type = []
        for index, value in enumerate(self.data_type):
            if index in require_index:
                data_type.append(value)

        new_dict = {}

        for key in self.main_table:
            current_value = self.main_table[key].value
            new_dict[key] = DbObject.DbObject([current_value[i] for i in require_index])

        return require_metadata, data_type, new_dict

    # Select this table based on the condition.
    # type parameter: str - conditions
    # rtype require_metadata: array - return the original metadata
    # rtype new_dic: dictionary - new dictionary that copy from this table but only with required columns
    def select(self, parameter):

        new_dict = {}
        conditions = ConditionObject.transform_condition_to_object(parameter)
        # If index exist, using index to select the condition in equal
        if self.exist_index(parameter):
            item_position = []
            for cond in conditions:
                for key in cond.equal_dict:
                    if key in self.index:
                        for value in cond.equal_dict[key]:
                            if self.index[key].get(value):
                                item_position.extend(self.index[key][value])

            for position in item_position:
                new_dict[position] = self.main_table[position]

        for cond in conditions:
            for key in self.main_table:
                if key not in new_dict:
                    current = self.main_table[key]
                    if ConditionObject.check_condition(cond, current, self.metadata):
                        new_dict[key] = current

        return self.metadata, self.data_type, new_dict

    # Input data from file.
    # type file_name: array - file name without type which exist in the rowData directory
    # type create_metadata_flag: boolean - to put the first line in the file as the metadata of this table
    def input_from_file(self, file_name, create_metadata_flag):

        for name in file_name:
            file = open('rowData/' + name + '.txt')
            line = file.readline().rstrip().strip().replace(" ", "")
            # Create metadata
            if create_metadata_flag:
                metadata_array = line.lower().split('|')
                self.assign_metadata(metadata_array)
                line = file.readline().rstrip().strip().replace(" ", "")

            # check data type:
            data_type = []
            line_item = line.split('|')
            for item in line_item:
                if GeneralFunction.check_is_float(item):
                    data_type.append('float')
                else:
                    data_type.append('string')

            self.data_type = data_type

            # Input data into dictionary
            iterator = 0
            while line:
                line_item = line.split('|')
                object_array = []
                for index, d_type in enumerate(data_type):
                    matching_value = line_item[index]
                    if d_type == 'float':
                        object_array.append(float(matching_value))
                    else:
                        object_array.append(matching_value)

                self.main_table[str(iterator)] = DbObject.DbObject(object_array)
                line = file.readline().rstrip().strip().replace(" ", "")
                iterator += 1
            file.close()

    # Sort this table by parameters in metadata.
    # type require_metadata: array - desired metadata which exist in this table
    # rtype require_metadata: array - return the input require_metadata
    # rtype new_dic: dictionary - new dictionary that copy from this table but only with required columns
    def sort(self, require_metadata):

        # get the index of parameters in metadata
        require_index = GeneralFunction.get_index_of_metadata(self.metadata, require_metadata)

        # print(list(self.main_table.values())[0].value[require_index[0]])
        # define prepare_sort function and import operator
        # import itemgetter to take input from list variables and sort

        sorted_table = sorted(self.main_table.items(), key=lambda x: operator.itemgetter(*require_index)(x[1].value))

        # covert the list of tuples into dictionary
        new_dict = GeneralFunction.convert_tuples_into_dic(sorted_table)

        return self.metadata, self.data_type, new_dict

    # Return the value of moving average in metadata.
    # type variables: array - first is header and second is moving average period of time n
    # rtype new_metadata: array - return the key and input header
    # rtype new_dic: dictionary - return the value of key and input header
    def mov_avg(self, variables):
        require_header = variables[0]
        period_of_time = int(variables[1])

        new_metadata = [item for item in self.metadata]
        new_metadata.append('mov_avg')
        # get the index of parameters in metadata
        require_index = GeneralFunction.get_index_of_metadata(self.metadata, [require_header])[0]
        new_data_type = [item for item in self.data_type]
        new_data_type.append(self.data_type[require_index])

        new_dict = {}
        # moving average counting
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
                moving_avg += current_dbobj.value[require_index]
                count += 1

            mov_avg_value = moving_avg/count
            new_value = [item for item in self.main_table[key].value]
            new_value.append(mov_avg_value)
            new_dict[key] = DbObject.DbObject(new_value)

        return new_metadata, new_data_type, new_dict

    # Return the value of moving sum in metadata.
    # type variables: array - first is header and second is moving sum window_size
    # rtype new_metadata: array - return the key and input header
    # rtype new_dic: dictionary - return the value of key and input header
    def mov_sum(self, variables):
        require_header = variables[0]
        window_size = int(variables[1])

        new_metadata = [item for item in self.metadata]
        new_metadata.append('mov_sum')
        # get the index of parameters in metadata
        require_index = GeneralFunction.get_index_of_metadata(self.metadata, [require_header])[0]
        new_data_type = [item for item in self.data_type]
        new_data_type.append(self.data_type[require_index])

        new_dict = {}
        # moving average counting
        # current_index as list(self.main_table.keys()).index(key)
        for key in self.main_table:
            current_index = list(self.main_table.keys()).index(key)
            moving_sum = 0
            # counting period_of_time and get new_dict[key]
            for j in range(0, window_size):
                index = current_index - j
                if index < 0:
                    break
                current_dbobj = list(self.main_table.values())[index]
                moving_sum += current_dbobj.value[require_index]

            new_value = [item for item in self.main_table[key].value]
            new_value.append(moving_sum)
            new_dict[key] = DbObject.DbObject(new_value)

        return new_metadata, new_data_type, new_dict

    # Return average value of the column.
    # type require_metadata: array - desired metadata which exist in this table
    # rtype require_metadata: array - return the input avg_ + require_metadata
    # rtype new_dic: dictionary - new dictionary that have key is none and value is the outcome average
    def average(self, require_metadata):

        # get the index of parameters in metadata
        require_index = GeneralFunction.get_index_of_metadata(self.metadata, require_metadata)
        new_data_type = self.data_type[0]

        total_sum = 0
        for key in self.main_table:
            total_sum += self.main_table[key].value[require_index[0]]
        total_average = total_sum / len(self.main_table)

        return ['avg_' + require_metadata[0]], new_data_type, {None: total_average}

    # Return sum of the sum_header and group by group_header.
    # type sum_header: str - the column to sum up
    # type group_header: array - the columns to group by
    # rtype array - return the group_header and 'sum_' + sum_header
    # rtype group_dict: dictionary - new dictionary which have key equal to the value of the column in group_header,
    # and value of group_header and sum of sum_header.
    def sum_group(self, sum_header, group_header):

        sum_index = GeneralFunction.get_index_of_metadata(self.metadata, [sum_header])
        # get the index of parameters in metadata
        group_index = GeneralFunction.get_index_of_metadata(self.metadata, group_header)

        new_data_type = [self.data_type[i] for i in group_index] + [self.data_type[i] for i in sum_index]

        group_dict = {}

        for key in self.main_table:
            new_key_string, group_dict = self.update_sum_group_dict(group_dict, key, group_index, sum_index)

        return [header for header in group_header] + ['sum_' + sum_header], new_data_type, group_dict

    # Return avg of the avg_header and group by group_header.
    # type avg_header: str - the column to count average
    # type group_header: array - the columns to group by
    # rtype array - return the group_header and 'avg_' + sum_header
    # rtype group_dict: dictionary - new dictionary which have key equal to the value of the column in group_header,
    # and value of group_header and sum of sum_header.
    def avg_group(self, avg_header, group_header):

        avg_index = GeneralFunction.get_index_of_metadata(self.metadata, [avg_header])
        # get the index of parameters in metadata
        group_index = GeneralFunction.get_index_of_metadata(self.metadata, group_header)

        new_data_type = [self.data_type[i] for i in group_index] + [self.data_type[i] for i in avg_index]

        group_dict = {}
        count_dict = {}

        for key in self.main_table:
            new_key_string, group_dict = self.update_sum_group_dict(group_dict, key, group_index, avg_index)

            if count_dict.get(new_key_string):
                count_dict[new_key_string] += 1
            else:
                count_dict[new_key_string] = 1

        for key in group_dict:
            group_value = group_dict[key]
            group_value[-1] = str(group_value[-1] / count_dict[key])
        
        return [header for header in group_header] + ['avg_' + avg_header], new_data_type, group_dict

    # Create index by input metadata.
    # type metadata: str - the column to create index
    # rtype None
    def create_index(self, metadata):

        # get the index of parameters in metadata
        require_index = GeneralFunction.get_index_of_metadata(self.metadata, [metadata])
        new_dic = {}

        for key in self.main_table:
            new_key = self.main_table[key].value[require_index[0]]
            if new_dic.get(new_key):
                new_dic[new_key].append(key)
            else:
                new_dic[new_key] = [key]

        self.index[metadata] = new_dic

    # Check if index exist.
    # type parameters: str
    # rtype Boolean
    def exist_index(self, parameters):
        for key in self.index:
            if key in parameters:
                return True

        return False

    # Update the dictionary in group function.
    # type group_dict: dict
    # type key: str - current key in main table
    # type group_index: array - group index arrays
    # type target_index: array - target index arrays
    # rtype str, dictionary
    def update_sum_group_dict(self, group_dict, key, group_index, target_index):
        new_key = []
        for index in group_index:
            new_key.append(self.main_table[key].value[index])

        new_key_string = str(new_key)
        if group_dict.get(new_key_string):
            value = group_dict[new_key_string]
            value[-1] = value[-1] + self.main_table[key].value[target_index[0]]
        else:
            group_dict[new_key_string] = [k for k in new_key] + [self.main_table[key].value[target_index[0]]]

        return new_key_string, group_dict

    # Update the dictionary in group function.
    # type group_dict: dict
    # type key: str - current key in main table
    # type group_index: array - group index arrays
    # type target_index: array - target index arrays
    # rtype str, dictionary
    def update_count_group_dict(self, group_dict, key, group_index, target_index):
        new_key = []
        count = 0

        for index in group_index:
            new_key.append(self.main_table[key].value[index])

        new_key_string = str(new_key)
        if group_dict.get(new_key_string):
            value = group_dict[new_key_string]
            value[-1] = str(int(value[-1]) + 1)
        else:
            group_dict[new_key_string] = [k for k in new_key] + ['1']

        return new_key_string, group_dict

    # concatenate the two tables based on the schema.
    # type con_table2: tuple - the table that concatenate with self.main_table
    # rtype new_dic: dictionary - new dictionary that copy from this table but only with required columns
    def concat(self, con_table2):
        new_dict = {}
        if self.metadata == con_table2.metedata:
            iterator = 0
            for key in self.main_table:
                new_dict[iterator] = self.main_table[key]
                iterator += 1

            for key in con_table2:
                new_dict[iterator] = con_table2[key]
                iterator += 1

        return self.metadata, self.data_type, new_dict

    # Join function.
    # type current_name: str - current table name to join
    # type source_name: str - source table name for join
    # type source_table: DbObject - group index arrays
    # type parameter: str
    # rtype new_metadata: array - return the two tables metadata
    # rtype new_dic: dictionary - new dictionary that join the two table based on the conditions
    def join(self, current_name, source_name, source_table, parameter):

        new_dict = {}
        conditions = ConditionObject.transform_condition_to_object(parameter)
        # If index exist, using index to select the condition in equal

        have_index = False
        for cond in conditions:
            for key in cond.equal_dict:
                table1_value = key.split('.')
                table1_name = table1_value[0]
                table1_column = table1_value[1]
                for source_value in cond.equal_dict[key]:
                    table2_value = source_value.split('.')
                    table2_name = table2_value[0]
                    table2_column = table2_value[1]

                    # index exist in current table
                    if table1_name == current_name and table1_column in self.index:
                        have_index = True
                        self.join_through_single_index(new_dict, self.index, self.main_table, source_table.main_table, source_table.metadata, table1_column, table2_column)

                    elif table2_name == current_name and table2_column in self.index:
                        have_index = True
                        self.join_through_single_index(new_dict, self.index, self.main_table, source_table.main_table, source_table.metadata, table2_column, table1_column)

                    # index exist in source table
                    elif table1_name == source_name and table1_column in source_table.index:
                        have_index = True
                        self.join_through_single_index(new_dict, source_table.index, source_table.main_table, self.main_table, self.metadata, table1_column, table2_column)

                    elif table2_name == source_name and table2_column in source_table.index:
                        have_index = True
                        self.join_through_single_index(new_dict, source_table.index, source_table.main_table, self.main_table, self.metadata, table2_column, table1_column)

        # remove equal condition if index already run it.
        if have_index:
            for cond in conditions:
                cond.equal_dict = {}

        for cond in conditions:
            if not cond.is_empty():
                for key in self.main_table:
                    current_main = self.main_table[key]
                    for source_key in source_table.main_table:
                        new_key = key + '_' + source_key
                        if new_key not in new_dict:
                            source_main = source_table.main_table[source_key]
                            if ConditionObject.check_join_condition(cond, current_name, current_main, self.metadata,
                                                                    source_main, source_table.metadata):
                                new_value = [item for item in current_main.value]
                                new_value.extend(source_main.value)
                                new_dict[new_key] = DbObject.DbObject(new_value)

        new_metadata = []
        new_data_type = []
        for index, item in enumerate(self.metadata):
            new_metadata.append(current_name + '_' + item)
            new_data_type.append(self.data_type[index])

        for index, item in enumerate(source_table.metadata):
            new_metadata.append(current_name + '_' + item)
            new_data_type.append(self.data_type[index])

        return new_metadata, new_data_type, new_dict

    # join through single index.
    @staticmethod
    def join_through_single_index(new_dict, index, main_table, source_table, source_metadata, table1_column, table2_column):
        source_require_index = GeneralFunction.get_index_of_metadata(source_metadata,
                                                                     [table2_column])
        table1_index = index[table1_column]

        for source_key in source_table:
            item_position = []
            source_v = source_table[source_key].value[source_require_index[0]]
            if source_v in table1_index:
                item_position.extend(table1_index[source_v])
            for position in item_position:
                new_value = main_table[position].value
                new_value.extend(source_table[source_key].value)
                new_dict[position + '_' + source_key] = DbObject.DbObject(new_value)

    # Return sum value of the column.
    # type require_metadata: array - desired metadata which exist in this table
    # rtype require_metadata: array - return the input sum_ + require_metadata
    # rtype new_dic: dictionary - new dictionary that have key is none and value is the outcome average
    def sum(self, require_metadata):

        # get the index of parameters in metadata
        require_index = GeneralFunction.get_index_of_metadata(self.metadata, require_metadata)

        total_sum = 0
        for key in self.main_table:
            total_sum += int(self.main_table[key].value[require_index[0]])

        print(total_sum)

        return ['sum_' + require_metadata[0]], [self.data_type[require_index[0]]], {None: total_sum}

    # Return sum value of the column.
    # type require_metadata: array - desired metadata which exist in this table
    # rtype require_metadata: array - return the input sum_ + require_metadata
    # rtype new_dic: dictionary - new dictionary that have key is none and value is the outcome average
    def count(self, require_metadata):
        return ['count_' + require_metadata[0]], ['float'], {None: len(self.main_table)}

    # Return count of the count_header and group by count_header.
    # type count_header: str - the column to count as count
    # type group_header: array - the columns to group by
    # rtype array - return the group_header and 'count_' + count_header
    # rtype group_dict: dictionary - new dictionary which have key equal to the value of the column in group_header,
    # and value of group_header and sum of count_header.
    def count_group(self, count_header, group_header):
        count_index = GeneralFunction.get_index_of_metadata(self.metadata, [count_header])
        # get the index of parameters in metadata
        group_index = GeneralFunction.get_index_of_metadata(self.metadata, group_header)

        new_data_type = [self.data_type[i] for i in group_index] + [self.data_type[i] for i in count_index]

        group_dict = {}
        count_dict = {}

        for key in self.main_table:
            new_key_string, group_dict = self.update_count_group_dict(group_dict, key, group_index, count_index)

            if count_dict.get(new_key_string):
                count_dict[new_key_string] += 1
            else:
                count_dict[new_key_string] = 1

        return [header for header in group_header] + ['count_' + count_header], new_data_type, group_dict