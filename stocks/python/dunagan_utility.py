
from collections import OrderedDict

class DunaganListUtility:

    @staticmethod
    def sort_and_return_list(list_to_sort):
        list_to_sort.sort()
        return list_to_sort

    @staticmethod
    def convert_list_to_value_and_index_tuple_list(list_to_convert):
        """
        Convert the passed in list_to_convert to a list containing tuples mapping each element's index to its value

        :param list_to_convert:
        :return:
        """
        index = 0
        converted_list = []
        for data_value in list_to_convert:
            # Create a tuple mapping the element's index to its value
            tuple = (index, data_value)
            converted_list.append(tuple)
            index = index + 1
        return converted_list






def sort_float_dictionary_ascending(dictionary_of_floats):
    """
    :param dict dictionary_of_floats:
    :return: OrderedDict
    """
    sorted_list_of_keys = sorted(dictionary_of_floats, key = lambda i: float(dictionary_of_floats[i]), reverse = False)
    sorted_ordered_dictionary = OrderedDict()
    for key in sorted_list_of_keys:
        sorted_ordered_dictionary[key] = dictionary_of_floats[key]
    return sorted_ordered_dictionary

def write_dictionary_to_file(sorted_dictionary, file_name, separator = ','):
    """
    :param OrderedDict sorted_dictionary:
    :param string file_name:
    :return:
    """
    file_to_write_to = open(file_name, 'w')
    for key, value in sorted_dictionary.items():
        file_to_write_to.write( '{0}{1}{2}\n'.format(key, separator, value) )
    file_to_write_to.close()