
from collections import OrderedDict

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