class GeneralFunction:

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
    @staticmethod
    def convert_tuples_into_dic(sorted_table):
        new_dict = {}
        for i in sorted_table:
            key = i[0]
            value = i[1]
            new_dict[key] = value
        return new_dict