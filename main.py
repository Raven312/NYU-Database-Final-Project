from databaseStructure import MyHashTable
from databaseStructure import MyBTreeTable
from generalFunction import GeneralFunction

# This table is for mapping the user input variable and actual table
parameter_assignment_table = {}

# default database type is hash
database_type = input('Please input \'hash\' or \'btree\' to choose the database type you want: ')
if database_type.lower() not in ['hash', 'btree']:
    print('Invalid input, database type will be default: hash')
    database_type = 'hash'


# Perform the action from the std input
# type assign_name: str
# type function_name: str
# type parameters: array
def perform_input_action(assign_name, function_name, variables):
    function_name = function_name.lower()

    # Create table in parameter table if the table not exist
    if not parameter_assignment_table.get(assign_name):
        if database_type == 'hash':
            parameter_assignment_table[assign_name] = MyHashTable.MyHashTable()
        else:
            parameter_assignment_table[assign_name] = MyBTreeTable.MyBTreeTable()

    # Below actions follow the steps :
    # Get the table from assignment table -> perform the action

    # Action: inputfromfile
    if function_name == 'inputfromfile':
        temp_table = parameter_assignment_table[assign_name]
        temp_table.input_from_file(variables, True)

    # Action: project
    if function_name == 'project':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table[table_parameter]
        # Passing parameter except the first value as first value is table_parameter
        meta_data, new_dict = temp_old_table.project(variables[1::])
        temp_new_table = parameter_assignment_table[assign_name]
        temp_new_table.metadata = meta_data
        temp_new_table.main_table = new_dict

    # Action: select
    if function_name == 'select':
        table_parameter = variables[0]
        temp_old_table = parameter_assignment_table[table_parameter]
        meta_data, new_dict = temp_old_table.select(variables[1])
        temp_new_table = parameter_assignment_table[assign_name]
        temp_new_table.metadata = meta_data
        temp_new_table.main_table = new_dict

    # Action: sort
    if function_name == 'sort':
        table_parameter= variables[0]
        temp_old_table = parameter_assignment_table[table_parameter]
        # Passing parameter except the first value as first value is table_parameter
        meta_data, new_dict = temp_old_table.sort(variables[1::])
        temp_new_table = parameter_assignment_table[assign_name]
        temp_new_table.meta_data = meta_data
        temp_new_table.main_table = new_dict


# __TODO__ Below block is for testing purpose only

inputString = 'R := inputfromfile(sales1)'
assignName, actionName, actionParameters = GeneralFunction.get_input_action(inputString)

perform_input_action(assignName, actionName, actionParameters)

inputString = 'R1 := select(R, (time > 49) and (time < 51) )'
assignName, actionName, actionParameters = GeneralFunction.get_input_action(inputString)

perform_input_action(assignName, actionName, actionParameters)

inputString = 'R2 := project(R1, saleid, qty, pricerange)'
assignName, actionName, actionParameters = GeneralFunction.get_input_action(inputString)

rTable = parameter_assignment_table['T2']
print(rTable.metadata)
for key in rTable.main_hash_dict:
    print(rTable.main_hash_dict[key].value)

rTable = parameter_assignment_table['R1'].main_table
print(len(rTable))