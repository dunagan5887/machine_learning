import unittest
from collections import OrderedDict
from symbolData import SymbolData
from symbolData import SymbolDataCollection

class TestSymbolData(unittest.TestCase):

    def test_init(self):
        expected_symbol = 'A'
        symbolDataTest = SymbolData(expected_symbol)
        test_symbol = symbolDataTest.symbol
        self.assertEqual(expected_symbol, test_symbol, 'A SymbolData object was initialized but its symbol was returned as {0} while expected to be {1}'.format(test_symbol, expected_symbol))

    def test_getTodayPrice(self):
        expected_today_price = 12.4
        symbolDataTest = SymbolData('A')
        symbolDataTest.setTodayPrice(expected_today_price)
        test_today_price = symbolDataTest.getTodayPrice()
        self.assertEqual(expected_today_price, test_today_price, 'A SymbolData object did not return the expected value when calling getTodayPrice(), expected {0} but returned {1}'.format(expected_today_price, test_today_price))

    def test_getSpanAverageByCode_and_getPercentageDeltaOffSpanAverage(self):
        testSymbolData = SymbolData('A')
        span_to_test = [1.34, 5.6, -3.5, -0.3, 2.2]
        span_expected_average = 1.068
        span_even_expected_average = 0.01333
        span_odd_expected_average = 2.65
        off_average_value_to_test = 1.12
        span_code = 'test_span'
        testSymbolData.initializeSpanByCode(span_code)
        first_expected_none_value = testSymbolData.getSpanAverageByCode(span_code)
        self.assertIsNone(first_expected_none_value, 'A None value was not returned when getting the average of a span with no values')
        second_expected_none_value = testSymbolData.getPercentageDeltaOffSpanAverage(span_code, off_average_value_to_test)
        self.assertIsNone(second_expected_none_value, 'A None value was not returned when getting the percentage delta off average of a span with no values')
        unit_label_prefix = 'units_'
        i = 0
        for value in span_to_test:
            unit_label_suffix = 'even' if ((i % 2) == 0) else 'odd'
            unit_label = unit_label_prefix + unit_label_suffix
            unit_labels_list = [unit_label, i]
            testSymbolData.addSpanValueByCode(span_code, unit_labels_list, value)
            i += 1
        # Test average for all units in span
        span_test_average = testSymbolData.getSpanAverageByCode(span_code)
        self.assertEqual(span_expected_average, span_test_average, 'The computed average for a test span for a test SymbolData object was expected to be {0} but was {1}'.format(span_expected_average, span_test_average))
        expected_delta_off_average = 0.0486891385768
        test_delta_off_average = testSymbolData.getPercentageDeltaOffSpanAverage(span_code, off_average_value_to_test)
        test_delta_off_average = round(test_delta_off_average, 13)
        self.assertEqual(expected_delta_off_average, test_delta_off_average, 'The computed delta percentage off average for a test SymbolData object was expected to be {0} but was {1}'.format(expected_delta_off_average, test_delta_off_average))
        # Test average for odd/even units in span
        even_units_label = unit_label_prefix + 'even'
        test_even_units_average = testSymbolData.getSpanAverageByCode(span_code, even_units_label)
        test_even_units_average = round(test_even_units_average, 5)
        self.assertEqual(test_even_units_average, span_even_expected_average)
        odd_units_label = unit_label_prefix + 'odd'
        test_odd_units_average = testSymbolData.getSpanAverageByCode(span_code, odd_units_label)
        self.assertEqual(test_odd_units_average, span_odd_expected_average)
        # Test off-average for odd/even units in span
        expected_off_even_average_value = 83.0
        expected_off_odd_average_value = -0.57736
        test_delta_off_even_average = testSymbolData.getPercentageDeltaOffSpanAverage(span_code, off_average_value_to_test, even_units_label)
        test_delta_off_even_average = round(test_delta_off_even_average, 1)
        test_delta_off_odd_average = testSymbolData.getPercentageDeltaOffSpanAverage(span_code, off_average_value_to_test, odd_units_label)
        test_delta_off_odd_average = round(test_delta_off_odd_average, 5)
        self.assertEqual(expected_off_even_average_value, test_delta_off_even_average)
        self.assertEqual(expected_off_odd_average_value, test_delta_off_odd_average)

    def test_getSpanDeltaByCode(self):
        testSymbolData = SymbolData('A')
        span_code = 'test_span'
        testSymbolData.initializeSpanByCode(span_code)
        span_to_test = [[1.34, 1.10], [5.6, 1.0], [-3.5, 5.6], [-0.3, -3.5], [2.01, -0.3], [2.34, 2.01]]
        unit_label_prefix = 'units_'
        i = 0
        for unit_data in span_to_test:
            unit_label_suffix = 'even' if ((i % 2) == 0) else 'odd'
            unit_label = unit_label_prefix + unit_label_suffix
            unit_labels_list = [unit_label, i]
            close_price = unit_data[0]
            open_price = unit_data[1]
            testSymbolData.addSpanValueByCode(span_code, unit_labels_list, close_price, open_price)
            i += 1
        expected_span_delta = 1.24
        test_span_delta = testSymbolData.getSpanDeltaByCode(span_code)
        test_span_delta = round(test_span_delta, 2)
        self.assertEqual(expected_span_delta, test_span_delta)
        # Test the even/odd span deltas
        even_units_label = unit_label_prefix + 'even'
        odd_units_label = unit_label_prefix + 'odd'
        expected_even_units_delta = 0.91
        expected_odd_units_delta = 1.34
        test_even_units_delta = testSymbolData.getSpanDeltaByCode(span_code, even_units_label)
        test_even_units_delta = round(test_even_units_delta, 2)
        test_odd_units_delta = testSymbolData.getSpanDeltaByCode(span_code, odd_units_label)
        test_odd_units_delta = round(test_odd_units_delta, 2)
        self.assertEqual(expected_even_units_delta, test_even_units_delta)
        self.assertEqual(expected_odd_units_delta, test_odd_units_delta)

class TestSymbolDataCollection(unittest.TestCase):
    def test_isSymbolInCollection(self):
        expected_symbol = 'A'
        newCollection = SymbolDataCollection()
        is_symbol_in_collection = newCollection.isSymbolInCollection(expected_symbol)
        self.assertFalse(is_symbol_in_collection, 'SymbolDataCollection did not return False for a symbol not yet in the collection, returned {0}'.format(is_symbol_in_collection))
        other_expected_symbol = 'B'
        newCollection.addSymbolToCollection(expected_symbol)
        is_other_symbol_in_collection = newCollection.isSymbolInCollection(other_expected_symbol)
        self.assertFalse(is_other_symbol_in_collection, 'Collection did not return False for a symbol not yet in the collection after a different symbol had been added, returned {0}'.format(is_other_symbol_in_collection))
        testSymbolData = newCollection.getSymbolData(expected_symbol)
        self.assertIsInstance(testSymbolData, SymbolData, 'An object of type SymbolData was not returned for a symbol which should be in the SymbolDataCollection, returned was {0}'.format(testSymbolData))
        test_symbol_from_collection = testSymbolData.symbol
        self.assertEqual(expected_symbol, test_symbol_from_collection, 'The symbol {0} from a SymbolData object returned from a SymbolDataCollection did not match the symbol passed into newCollection.getSymbolData() {1}'.format(test_symbol_from_collection, expected_symbol))
        newCollection.addSymbolToCollection(other_expected_symbol)
        otherSymbolData = newCollection.getSymbolData(other_expected_symbol)
        other_symbol_from_data = otherSymbolData.symbol
        self.assertEqual(other_symbol_from_data, other_expected_symbol, 'The symbol {0} from another SymbolData object returned from a SymbolDataCollection did not match the other symbol passed into newCollection.getSymbolData() {1}'.format(other_symbol_from_data, other_expected_symbol))

    def test_getSortedSpanDeltaValuesByCode(self):
        expected_sorted_by_delta_dict = OrderedDict()
        expected_sorted_by_delta_dict['B'] = -9.1
        expected_sorted_by_delta_dict['C'] = -3.5
        expected_sorted_by_delta_dict['A'] = 0.9
        expected_sorted_by_delta_dict['D'] = 13.0
        span_code = 'test_sorted_span_deltas'
        test_unit_label_list = ['test_unit_code']
        other_unit_label_list = ['some_other_label']
        newCollection = SymbolDataCollection()
        testSymbolDataA = newCollection.addSymbolToCollection('A')
        testSymbolDataA.initializeSpanByCode(span_code)
        testSymbolDataA.addSpanValueByCode(span_code, test_unit_label_list, 1.0, 0.6)
        testSymbolDataA.addSpanValueByCode(span_code, test_unit_label_list, 1.5, 1.0)
        testSymbolDataA.addSpanValueByCode(span_code, other_unit_label_list, 1.4, 1.5)
        testSymbolDataB = newCollection.addSymbolToCollection('B')
        testSymbolDataB.initializeSpanByCode(span_code)
        testSymbolDataB.addSpanValueByCode(span_code, test_unit_label_list, -4.0, 0.6)
        testSymbolDataB.addSpanValueByCode(span_code, test_unit_label_list, -8.5, 4.0)
        testSymbolDataB.addSpanValueByCode(span_code, other_unit_label_list, 4.5, -8.5)
        testSymbolDataC = newCollection.addSymbolToCollection('C')
        testSymbolDataC.initializeSpanByCode(span_code)
        testSymbolDataC.addSpanValueByCode(span_code, test_unit_label_list, 1.2, 5.7)
        testSymbolDataC.addSpanValueByCode(span_code, test_unit_label_list, 2.2, 1.2)
        testSymbolDataC.addSpanValueByCode(span_code, other_unit_label_list, 1.2, 2.2)
        testSymbolDataD = newCollection.addSymbolToCollection('D')
        testSymbolDataD.initializeSpanByCode(span_code)
        testSymbolDataD.addSpanValueByCode(span_code, test_unit_label_list, 4.0, -8.6)
        testSymbolDataD.addSpanValueByCode(span_code, test_unit_label_list, 4.4, 4.0)
        testSymbolDataD.addSpanValueByCode(span_code, other_unit_label_list, 2.3, 4.4)
        testOtherLabelOnlySymbolData = newCollection.addSymbolToCollection('E')
        testOtherLabelOnlySymbolData.initializeSpanByCode(span_code)
        testOtherLabelOnlySymbolData.addSpanValueByCode(span_code, other_unit_label_list, 3.4, 5.9)
        sorted_span_delta_values_by_code_dict = newCollection.getSortedSpanDeltaValuesByCode(span_code, 'test_unit_code')
        self.assertEqual(expected_sorted_by_delta_dict, sorted_span_delta_values_by_code_dict, 'test_getSortedDeltaValuesByCode failed to return the expected sorted dictionary, returned {0}\nexpected: {1}'.format(sorted_span_delta_values_by_code_dict, expected_sorted_by_delta_dict))

    def test_getSortedTodayPricePercentageOffSpanAveragesByCode(self):
        test_span_code = 'percentage_price_off_span_average_test'
        testCollection = SymbolDataCollection()
        first_today_price = 2.3
        first_span_prices = [5.7, 8.9, 7.7]
        second_today_price = 3.4
        second_span_prices = [2.3, 2.7, 3.3]
        third_today_price = 1.2
        third_span_prices = [4.6, 2.7, 5.6]
        firstSymbolData = testCollection.addSymbolToCollection('A')
        firstSymbolData.setTodayPrice(first_today_price)
        firstSymbolData.initializeSpanByCode(test_span_code)
        unit_label_prefix = 'units_'
        i = 0
        for price in first_span_prices:
            unit_label_suffix = 'even' if ((i % 2) == 0) else 'odd'
            unit_label = unit_label_prefix + unit_label_suffix
            unit_labels_list = [unit_label, i]
            firstSymbolData.addSpanValueByCode(test_span_code, unit_labels_list, price)
            i += 1
        secondSymbolData = testCollection.addSymbolToCollection('B')
        secondSymbolData.initializeSpanByCode(test_span_code)
        secondSymbolData.setTodayPrice(second_today_price)
        i = 0
        for price in second_span_prices:
            unit_label_suffix = 'even' if ((i % 2) == 0) else 'odd'
            unit_label = unit_label_prefix + unit_label_suffix
            unit_labels_list = [unit_label, i]
            secondSymbolData.addSpanValueByCode(test_span_code, unit_labels_list, price)
            i += 1
        thirdSymbolData = testCollection.addSymbolToCollection('C')
        thirdSymbolData.initializeSpanByCode(test_span_code)
        thirdSymbolData.setTodayPrice(third_today_price)
        i = 0
        for price in third_span_prices:
            unit_label_suffix = 'even' if ((i % 2) == 0) else 'odd'
            unit_label = unit_label_prefix + unit_label_suffix
            unit_labels_list = [unit_label, i]
            thirdSymbolData.addSpanValueByCode(test_span_code, unit_labels_list, price)
            i += 1

        testNoneSymbolData = testCollection.addSymbolToCollection('D')
        testNoneSymbolData.initializeSpanByCode(test_span_code)

        test_price_off_average_values_by_symbol = testCollection.getSortedTodayPricePercentageOffSpanAveragesByCode(test_span_code)
        expected_price_off_average_values_by_symbol = OrderedDict()
        expected_price_off_average_values_by_symbol['C'] = -0.7209302325581395
        expected_price_off_average_values_by_symbol['A'] = -0.6905829596412556
        expected_price_off_average_values_by_symbol['B'] = 0.2289156626506022
        self.assertEqual(test_price_off_average_values_by_symbol, expected_price_off_average_values_by_symbol, 'The expected_price_off_average_values_by_symbol was {0}, but {1} was returned while testing getting the percentage delta of today price off of span average'.format(expected_price_off_average_values_by_symbol, test_price_off_average_values_by_symbol))
        price_threshold = 2.0
        test_sorted_dictionary_of_values_above_price_threshold = testCollection.getSortedDictionaryOfValuesAboveTodayPriceThreshold(test_price_off_average_values_by_symbol, price_threshold)
        expected_price_off_average_values_by_symbol_above_threshold = OrderedDict()
        expected_price_off_average_values_by_symbol_above_threshold['A'] = -0.6905829596412556
        expected_price_off_average_values_by_symbol_above_threshold['B'] = 0.2289156626506022
        self.assertEqual(test_sorted_dictionary_of_values_above_price_threshold, expected_price_off_average_values_by_symbol_above_threshold, 'The expected_price_off_average_values_by_symbol_above_threshold was {0} but {1} was returned'.format(expected_price_off_average_values_by_symbol_above_threshold, test_sorted_dictionary_of_values_above_price_threshold))
        # Test for even/odd unit labels
        even_units_label = unit_label_prefix + 'even'
        odd_units_label = unit_label_prefix + 'odd'
        test_price_off_even_average_values_by_symbol = testCollection.getSortedTodayPricePercentageOffSpanAveragesByCode(test_span_code, even_units_label)
        test_price_off_odd_average_values_by_symbol = testCollection.getSortedTodayPricePercentageOffSpanAveragesByCode(test_span_code, odd_units_label)
        expected_price_off_even_average_values = OrderedDict()
        expected_price_off_even_average_values['C'] = -0.7647058823529411
        expected_price_off_even_average_values['A'] = -0.6567164179104478
        expected_price_off_even_average_values['B'] = 0.21428571428571433
        self.assertEqual(expected_price_off_even_average_values, test_price_off_even_average_values_by_symbol)
        expected_price_off_odd_average_values = OrderedDict()
        expected_price_off_odd_average_values['A'] = -0.7415730337078652
        expected_price_off_odd_average_values['C'] = -0.5555555555555556
        expected_price_off_odd_average_values['B'] = 0.25925925925925913
        self.assertEqual(test_price_off_odd_average_values_by_symbol, expected_price_off_odd_average_values)
        test_sorted_dictionary_of_even_values_above_price_threshold = testCollection.getSortedDictionaryOfValuesAboveTodayPriceThreshold(test_price_off_even_average_values_by_symbol, price_threshold)
        expected_price_off_even_average_values_by_symbol_above_threshold = OrderedDict()
        expected_price_off_even_average_values_by_symbol_above_threshold['A'] = -0.6567164179104478
        expected_price_off_even_average_values_by_symbol_above_threshold['B'] = 0.21428571428571433
        self.assertEqual(expected_price_off_even_average_values_by_symbol_above_threshold, test_sorted_dictionary_of_even_values_above_price_threshold)
        test_sorted_dictionary_of_odd_values_above_price_threshold = testCollection.getSortedDictionaryOfValuesAboveTodayPriceThreshold(test_price_off_odd_average_values_by_symbol, price_threshold)
        expected_price_off_odd_average_values_by_symbol_above_threshold = OrderedDict()
        expected_price_off_odd_average_values_by_symbol_above_threshold['A'] = -0.7415730337078652
        expected_price_off_odd_average_values_by_symbol_above_threshold['B'] = 0.25925925925925913
        self.assertEqual(expected_price_off_odd_average_values_by_symbol_above_threshold, test_sorted_dictionary_of_odd_values_above_price_threshold)

if __name__ == '__main__':
    unittest.main()