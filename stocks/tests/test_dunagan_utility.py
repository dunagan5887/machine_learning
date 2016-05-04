import unittest
from collections import OrderedDict
from dunagan_utility import write_dictionary_to_file


class TestDunaganUtility(unittest.TestCase):

    def test_write_dictionary_to_file(self):
        expected_price_off_average_values_by_symbol = OrderedDict()
        expected_price_off_average_values_by_symbol['C'] = -0.7209302325581395
        expected_price_off_average_values_by_symbol['A'] = -0.6905829596412556
        expected_price_off_average_values_by_symbol['B'] = 0.2289156626506022
        file_to_write_to = 'test_output/test_write_to_dictionary'
        expected_file_data = "C,-0.720930232558\nA,-0.690582959641\nB,0.228915662651\n"
        write_dictionary_to_file(expected_price_off_average_values_by_symbol, file_to_write_to)
        test_file = open(file_to_write_to, 'r')
        test_file_data = test_file.read()
        test_file.close()
        self.assertEqual(expected_file_data, test_file_data, 'The data written to a file for a dictionary did not match the expected data')


if __name__ == '__main__':
    unittest.main()
