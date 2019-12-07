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
        elif 'btree' in input_string or 'hash' in input_string or 'outputtofile' in input_string:
            # Get the function name.
            function_name = input_string[0:parameter_start_index]

        # Get the parameter for this function.
        parameters = input_string[parameter_start_index + 1:len(input_string) - 1].split(',')
        return assign_name, function_name, parameters

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
    # type function_name: str
    # type input_string: str
    @staticmethod
    def print_time(start, end, function_name, input_string):
        print("Execute: {:<50} , Function Name: {:<15}, Run time: {}".format(input_string, function_name, str(end - start)))

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

    # covert the list of tuples into dictionary
    # type sorted_table: tuples
    # type new_dict: dictionary

    @staticmethod
    def check_is_float(value):
        if value.isnumeric():
            return True
        else:
            try:
                float(value)
                return True
            except ValueError:
                return False

