from generalFunction import GeneralFunction
from ParameterAssignmentTable import ParameterAssignmentTable

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
def perform_input_action(assign_name, function_name, variables):
    function_name = function_name.lower()

    # Create table in parameter table if the table not exist
    parameter_assignment_table.create_parameter_assignment_table(assign_name)

    # Below actions follow the steps :
    # Get the table from assignment table -> perform the action

    # Action: inputfromfile
    if function_name == 'inputfromfile':
        temp_table = parameter_assignment_table.get_parameter_assignment_table(assign_name)
        temp_table.input_from_file(variables, True)

    # Action: project
    if function_name == 'project':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
        # Passing parameter except the first value as first value is table_parameter
        meta_data, new_dict = temp_old_table.project(variables[1::])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, new_dict)

    # Action: select
    if function_name == 'select':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
        meta_data, new_dict = temp_old_table.select(variables[1])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, new_dict)

    # Action: sort
    if function_name == 'sort':
        table_parameter= variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
        # Passing parameter except the first value as first value is table_parameter
        meta_data, new_dict = temp_old_table.sort(variables[1::])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, new_dict)

    # Action: movavg
    if function_name == 'movavg':
        table_parameter= variables[0]
        temp_old_table = parameter_assignment_table[table_parameter]
        # Passing parameter except the first value as first value is table_parameter
        meta_data, new_dict = temp_old_table.mov_avg(variables[1::])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, new_dict)

    # Action: avg
    if function_name == 'avg':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
        # Passing parameter except the first value as first value is table_parameter
        meta_data, new_dict = temp_old_table.average(variables[1::])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, new_dict)

    # Action: avg
    if function_name == 'sumgroup':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table.get_parameter_assignment_table(table_parameter)
        # Passing parameter except the first value as first value is table_parameter
        meta_data, new_dict = temp_old_table.sum_group(variables[1], variables[2::])
        parameter_assignment_table.insert_parameter_assignment_table(assign_name, meta_data, new_dict)


# __TODO__ Below block is for testing purpose only

inputString = 'R1 := inputfromfile(test)'
assignName, actionName, actionParameters = GeneralFunction.get_input_action(inputString)

perform_input_action(assignName, actionName, actionParameters)

inputString = 'T3 :=  sort(R1, time, pricerange)'
assignName, actionName, actionParameters = GeneralFunction.get_input_action(inputString)

perform_input_action(assignName, actionName, actionParameters)

rTable = parameter_assignment_table.get_parameter_assignment_table('T3').main_table
print(len(rTable))
