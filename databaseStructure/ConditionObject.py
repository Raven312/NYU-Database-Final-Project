import operator


# Condition object
class ConditionObject:
    def __init__(self, equal_dict, less_dict, greater_dict, not_equal_dict, greater_equal_dict, less_equal_dict):
        self.equal_dict = equal_dict
        self.less_dict = less_dict
        self.greater_dict = greater_dict
        self.not_equal_dict = not_equal_dict
        self.greater_equal_dict = greater_equal_dict
        self.less_equal_dict = less_equal_dict

    # Check if the db object fulfill the condition object.
    # type cond: ConditionObject
    # type db_object: DbObject
    # type metadata: array
    # rtype boolean
    @staticmethod
    def check_condition(cond, db_object, metadata):

        if not apply_condition(cond.equal_dict, '!=', metadata, db_object):
            return False
        if not apply_condition(cond.less_dict, '>=', metadata, db_object):
            return False
        if not apply_condition(cond.greater_dict, '<=', metadata, db_object):
            return False
        if not apply_condition(cond.not_equal_dict, '==', metadata, db_object):
            return False
        if not apply_condition(cond.greater_equal_dict, '<', metadata, db_object):
            return False
        if not apply_condition(cond.less_equal_dict, '>', metadata, db_object):
            return False

        return True


# Check if the db object fulfill the single condition dictionary.
# type source_dict: ConditionObject
# type op: str
# type metadata: array
# type db_object: DbObject
# rtype boolean
def apply_condition(source_dict, op, metadata, db_object):
    ops = {'==': operator.eq, '>=': operator.ge, '<=': operator.le,
           '!=': operator.ne, '>': operator.gt, '<': operator.lt}

    op_func = ops[op]
    for key in source_dict:
        index = metadata.index(key)
        for value in source_dict[key]:
            if op_func(db_object.value[index], value):
                return False

    return True
