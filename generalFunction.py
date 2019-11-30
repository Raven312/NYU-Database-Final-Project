from ConditionObject import ConditionObject


class GeneralFunction:

    # Get the action information from action.
    # type input_string: str - user input string
    # rtype assign_name: str
    # rtype function_name: str
    # rtype parameters: array
    @staticmethod
    def get_input_action(input_string):
        input_string = input_string.rstrip().strip().replace(" ", "").lower()
        assign_name = ''
        function_name = ''
        parameter_start_index = input_string.index("(")

        # '(' and ':=' are two key symbol that must exist in the input.
        if '(' in input_string and ':=' in input_string:
            # Get the table name.
            assign_name_index = input_string.index(":=")
            assign_name = input_string[0:assign_name_index]
            # Get the function name.
            function_name = input_string[assign_name_index + 2:parameter_start_index]
        elif 'btree' in input_string or 'hash' in input_string:
            # Get the function name.
            function_name = input_string[0:parameter_start_index]

        # Get the parameter for this function.
        parameters = input_string[parameter_start_index + 1:len(input_string) - 1].split(',')
        return assign_name, function_name, parameters

    # Transform condition string to array ConditionObject.
    # 'And' condition will be put into a single object and 'Or' condition will be put into separate object
    # type variables: str - user input conditions
    # rtype result: array of ConditionObject
    @staticmethod
    def transform_condition_to_object(variables):
        variables = variables.lower().replace('(', '').replace(')', '')
        result = []
        conditions = []

        variables_or = variables.split('or')
        for variable in variables_or:
            variables_and = variable.split('and')
            conditions.append(variables_and)

        for cond in conditions:
            equal_dict = {}
            less_dict = {}
            greater_dict = {}
            not_equal_dict = {}
            greater_equal_dict = {}
            less_equal_dict = {}

            for item in cond:
                if '=' in item and '>' not in item and '<' not in item and '!' not in item:
                    append_to_dict(equal_dict, '=', item)
                elif '>=' in item:
                    append_to_dict(greater_equal_dict, '>=', item)
                elif '<=' in item:
                    append_to_dict(less_equal_dict, '<=', item)
                elif '>' in item:
                    append_to_dict(greater_dict, '>', item)
                elif '<' in item:
                    append_to_dict(less_dict, '<', item)
                elif '!=' in item:
                    append_to_dict(not_equal_dict, '!=', item)

            result.append(
                ConditionObject(equal_dict, less_dict, greater_dict,
                                not_equal_dict, greater_equal_dict, less_equal_dict))

        return result

    # Get the index of required metadata.
    # type metadata: array - resource metadata
    # type match_data: array - target metadata
    # rtype result: array - return the index of the target metadata in the resource metadata
    @staticmethod
    def get_index_of_metadata(metadata, match_data):
        result = []
        for md in match_data:
            for index, value in enumerate(metadata):
                if md == value:
                    result.append(index)
        return result

    # Print the time in the std output.
    # type start: float - start time
    # type end: float - end time
    @staticmethod
    def print_time(start, end):
        print(end - start)

    # covert the list of tuples into dictionary
    # type sorted_table: tuples
    # type new_dict: dictionary
    @staticmethod
    def convert_tuples_into_dic(sorted_table):
        new_dict = {}
        for i in sorted_table:
            key = i[0]
            value = i[1]
            new_dict[key] = value
        return new_dict


# Append the value to target dictionary.
# type target_dict: dictionary
# type operator: str
# type item: str - user input condition
def append_to_dict(target_dict, operator, item):
    values = item.split(operator)
    if target_dict.get(values[0]):
        target_dict[values[0]] = target_dict[values].append(values[0])
    else:
        target_dict[values[0]] = [values[1]]
