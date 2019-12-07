from GeneralFunction import GeneralFunction
from ParameterAssignmentTable import ParameterAssignmentTable
import time

# This table is for mapping the user input variable and actual table
parameter_assignment_table = ParameterAssignmentTable()

# default database type is hash
database_type = input('Please input \'hash\' or \'btree\' to choose the database type you want: ')
if database_type.lower() not in ['hash', 'btree']:
    print('Invalid input, database type will be default: hash')
    database_type = 'hash'

parameter_assignment_table.set_database_type(database_type)


# Perform the action from the std input
# type assign_name: str
# type function_name: str
# type parameters: array
def perform_input_action(assign_name, function_name, variables, input_string):
    function_name = function_name.lower()

    start = time.time()

    # Create table in parameter table if the table not exist
    if function_name != 'hash' or function_name != 'hash':
        parameter_assignment_table.create_parameter_assignment_table(assign_name)

    # Below actions follow the steps :
    # Get the table from assignment table -> perform the action

    # Action: inputfromfile
    if function_name == 'inputfromfile':
        temp_table = parameter_assignment_table.get_parameter_assignment_table(assign_name)
        temp_table.input_from_file(variables, True)
        temp_table.assign_name(assign_name)

    # Action: project
    if function_name == 'project':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
        # Passing parameter except the first value as first value is table_parameter
        meta_data, data_type, new_dict = temp_old_table.project(variables[1::])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, data_type, new_dict)

    # Action: select
    if function_name == 'select':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
        meta_data, data_type, new_dict = temp_old_table.select(variables[1])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, data_type, new_dict)

    # Action: sort
    if function_name == 'sort':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
        # Passing parameter except the first value as first value is table_parameter
        meta_data, data_type, new_dict = temp_old_table.sort(variables[1::])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, data_type, new_dict)

    # Action: movavg
    if function_name == 'movavg':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
        # Passing parameter except the first value as first value is table_parameter
        meta_data, data_type, new_dict = temp_old_table.mov_avg(variables[1::])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, data_type, new_dict)

    # Action: avg
    if function_name == 'avg':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
        # Passing parameter except the first value as first value is table_parameter
        meta_data, data_type, new_dict = temp_old_table.average(variables[1::])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, data_type, new_dict)

    # # Action: movsum
    # if function_name == 'movsum':
    #     table_parameter = variables[0]
    #     temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
    #     # Passing parameter except the first value as first value is table_parameter
    #     meta_data, data_type, new_dict = temp_old_table.mov_sum(variables[1::])
    #     parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, data_type, new_dict)

    # Action: sumgroup
    if function_name == 'sumgroup':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
        # Passing parameter except the first value as first value is table_parameter
        meta_data, data_type, new_dict = temp_old_table.sum_group(variables[1], variables[2::])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, data_type, new_dict)

    # Action: avggroup
    if function_name == 'avggroup':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
        # Passing parameter except the first value as first value is table_parameter
        meta_data, data_type, new_dict = temp_old_table.avg_group(variables[1], variables[2::])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, data_type, new_dict)

    # Actions: create index
    if function_name == 'hash' or function_name == 'btree':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
        # Passing parameter except the first value as first value is table_parameter
        temp_old_table.create_index(variables[1])

    # Action: join
    if function_name == 'join':
        table_parameter1 = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter1)
        table_parameter2 = variables[1]
        temp_old_table2 = parameter_assignment_table.get_parameter_assignment_table(table_parameter2)
        meta_data, data_type, new_dict = temp_old_table.join(table_parameter1, table_parameter2, temp_old_table2, variables[2])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, data_type, new_dict)

    # Action: concat
    if function_name == 'concat':
        table_parameter1 = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter1)
        table_parameter2 = variables[1]
        temp_old_table2 = parameter_assignment_table.get_parameter_assignment_table(table_parameter2)
        meta_data, data_type, new_dict = temp_old_table.concat(table_parameter2, temp_old_table2, variables[2])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, data_type, new_dict)

    # Action: sum
    if function_name == 'sum':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
        # Passing parameter except the first value as first value is table_parameter
        meta_data, data_type, new_dict = temp_old_table.sum(variables[1::])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, data_type, new_dict)

    # Action: count
    if function_name == 'count':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
        # Passing parameter except the first value as first value is table_parameter
        meta_data, data_type, new_dict = temp_old_table.count(variables[1::])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, data_type, new_dict)

    # Action: countgroup
    if function_name == 'countgroup':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
        # Passing parameter except the first value as first value is table_parameter
        meta_data, data_type, new_dict = temp_old_table.count_group(variables[1], variables[2::])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, data_type, new_dict)

    if function_name == 'outputtofile':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
        with open('output/' + variables[1] + '.txt', 'w') as file:
            # write metadata at first line
            meta_data_string = ''
            for value in temp_old_table.metadata:
                meta_data_string = meta_data_string + value + '|'
            meta_data_string = meta_data_string[0: len(meta_data_string) - 1]
            file.writelines(meta_data_string + '\n')

            # write values
            for key in temp_old_table.main_table:
                value_string = ''
                current_obj = temp_old_table.main_table[key]
                for value in current_obj.value:
                    value_string = value_string + value + '|'
                value_string = value_string[0:len(value_string) - 1]
                file.writelines(value_string + '\n')

        file.close()

    GeneralFunction.print_time(start, time.time(), function_name, input_string)


# Read test file by user input file name
def read_test(file_name):
    file = open(file_name)
    line = file.readline()
    while line:
        # Clean space and change line symbol
        line = line.rstrip().strip().replace(" ", "")
        # Get key information
        assign_name, action_name, action_parameters = GeneralFunction.get_input_action(line)
        # Perform db action
        perform_input_action(assign_name, action_name, action_parameters, line)

        line = file.readline()
    file.close()


test_file_name = input("Please copy paste the test file to the home directory of this program and input the file name:")

read_test(test_file_name)
