from databaseStructure import MyHashTable
from databaseStructure import MyBTreeTable

# This table is for mapping the user input variable and actual table
parameter_assignment_table = {}

# default database type is hash
database_type = input('Please input \'hash\' or \'btree\' to choose the database type you want: ')
if database_type.lower() not in ['hash', 'btree']:
    print('Invalid input, database type will be default: hash')
    database_type = 'hash'


# Get the action information from action.
def get_input_action(input_string):
    input_string = input_string.rstrip().strip().replace(" ", "")
    # '(' and ':=' are two key symbol that must exist in the input.
    if '(' in input_string and ':=' in input_string:

        # Get the table name.
        assign_name_index = input_string.index(":=")
        assign_name = input_string[0:assign_name_index]

        # Get the function name.
        parameter_start_index = input_string.index("(")
        function_name = input_string[assign_name_index + 2:parameter_start_index]

        # Get the parameter for this function.
        parameters = input_string[parameter_start_index + 1:len(input_string) - 1].split(',')

        return assign_name, function_name, parameters
    return None, None, None


# Perform the action from the std input
def perform_input_action(assign_name, function_name, variables):
    function_name = function_name.lower()

    # Create table in parameter table if the table not exist
    if not parameter_assignment_table.get(assign_name):
        if database_type == 'hash':
            parameter_assignment_table[assign_name] = MyHashTable.MyHashTable()
        else:
            parameter_assignment_table[assign_name] = MyBTreeTable.MyBTreeTable()

    # Below actions follow the steps :
    # Get the table from assignment table -> perform the action -> put it back to assignment table

    # Action: inputfromfile
    if function_name == 'inputfromfile':
        temp_table = parameter_assignment_table[assign_name]
        temp_table.input_from_file(variables, True)
        parameter_assignment_table[assign_name] = temp_table

    # Action: project
    if function_name == 'project':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table[table_parameter]
        # Passing parameter except the first value as first value is table_parameter
        meta_data, new_dict = temp_old_table.project(variables[1::])
        temp_new_table = parameter_assignment_table[assign_name]
        temp_new_table.meta_data = meta_data
        temp_new_table.main_hash_dict = new_dict

    # Action: sort
    if function_name == 'sort':
        table_parameter= variables[0]
        temp_old_table = parameter_assignment_table[table_parameter]
        # Passing parameter except the first value as first value is table_parameter
        meta_data, new_dict = temp_old_table.sort(variables[1::])
        temp_new_table = parameter_assignment_table[assign_name]
        temp_new_table.meta_data = meta_data
        temp_new_table.main_hash_dict = new_dict








# __TODO__ Below block is for testing purpose only
inputString = 'R1 := inputfromfile(sales1)'
assignName, actionName, actionParameters = get_input_action(inputString)

perform_input_action(assignName, actionName, actionParameters)

inputString = 'R2 := project(R1, saleid, qty, pricerange)'
assignName, actionName, actionParameters = get_input_action(inputString)

perform_input_action(assignName, actionName, actionParameters)

inputString = 'T2 := sort(T1, S_C)'
assignNmae, actionName, actionParameters = get_input_action(inputString)

perform_input_action(assignNmae, actionName, actionParameters)

rTable = parameter_assignment_table['R1']
print(rTable)



