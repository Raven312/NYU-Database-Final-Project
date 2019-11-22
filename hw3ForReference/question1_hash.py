from databaseStructure import MyHashTable, DbObject


# Input myindex file into our database
def read_my_index():
    file = open('rowData/myindex.txt')
    line = file.readline()
    if not line.split('|')[0].isnumeric():
        line = file.readline()
    while line:
        line_item = line.split('|')
        hash_database.main_hash_dict[line_item[0]] = DbObject.DbObject(line_item[1])
        line = file.readline()
    file.close()


# Read test file by user input file name
def read_test(file_name):
    file = open(file_name)
    line = file.readline()
    while line:
        # Clean space and change line symbol
        line = line.rstrip().strip().replace(" ", "")
        # Get key information
        function_name, variable = get_input_action(line)
        # Perform db action
        perform_input_action(function_name, variable)
        line = file.readline()
    file.close()
    print()
    print('Total time:' + str(hash_database.time_elapsed))


# Get the key information (Ex: insert, search, delete and variable) from action.
def get_input_action(input_string):
    if '(' in input_string:
        a = input_string.index("(")
        name = input_string[:a]
        sql = input_string[a + 1:len(input_string) - 1]
        return name, sql
    return input_string, None


# Perform database actions by key information
def perform_input_action(function_name, variable):
    function_name = function_name.lower()

    if function_name == 'search':
        print(hash_database.search(variable))

    elif function_name == 'insert':
        # As insert have two variable, use split to separate them.
        multi_variables = variable.split(',')
        print(hash_database.insert(multi_variables[0], multi_variables[1]))

    elif function_name == 'delete':
        print(hash_database.delete(variable))


hash_database = MyHashTable.MyHashTable()

# program start from here
# read my index file from local
read_my_index()

test_file_name = input("Please copy paste the test file to the home directory of this program and input the file name:")

read_test(test_file_name)

hash_database.write_file()
hash_database.write_time_file()
