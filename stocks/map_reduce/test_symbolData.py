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

    def test_getDeltaByCode(self):
        testSymbolData = SymbolData('A')
        expected_before_delta = 4.5
        expected_after_delta = 3.2
        expected_delta = -1.3
        delta_code = 'test_delta'
        testSymbolData.initializeDeltaByCode(delta_code)
        testSymbolData.setDeltaBeforeByCode(delta_code, expected_before_delta)
        testSymbolData.setDeltaAfterByCode(delta_code, expected_after_delta)
        test_delta = testSymbolData.getDeltaByCode(delta_code)
        test_delta = round(test_delta,1)
        self.assertEqual(expected_delta, test_delta, 'The computed delta for a test SymbolData object was expected to be {0} but was {1}'.format(expected_delta, test_delta))
        test_before_delta = testSymbolData.getDeltaBeforeByCode(delta_code)
        self.assertEqual(expected_before_delta, test_before_delta, 'The delta before value returned from a test SymbolData object was expected to be {0} but instead was {1}'.format(expected_before_delta, test_before_delta))
        test_after_delta = testSymbolData.getDeltaAfterByCode(delta_code)
        self.assertEqual(expected_after_delta, test_after_delta, 'The delta after value returned from a test SymbolData object was expected to be {0} but was instead {1}'.format(expected_after_delta, test_after_delta))
        incomplete_delta_code = 'incomplete_delta'
        testSymbolData.initializeDeltaByCode(incomplete_delta_code)
        first_expected_false_value = testSymbolData.getDeltaAfterByCode(incomplete_delta_code)
        self.assertFalse(first_expected_false_value, 'A value of False was not returned for a delta after valuw which was never declared, instead {0} was returned'.format(first_expected_false_value))
        testSymbolData.setDeltaAfterByCode(incomplete_delta_code, expected_after_delta)
        expected_none_value = testSymbolData.getDeltaByCode(incomplete_delta_code)
        self.assertIsNone(expected_none_value, 'A value of None was not returned for a delta value which was incomplete, the returned value was {0}'.format(expected_none_value))
        second_expected_false_value = testSymbolData.getDeltaBeforeByCode(incomplete_delta_code)
        self.assertFalse(second_expected_false_value, 'A value of False was not returned for a delta before value which was never declared, {0} was returned'.format(second_expected_false_value))

    def test_getSpanAverageByCode_and_getPercentageDeltaOffSpanAverage(self):
        testSymbolData = SymbolData('A')
        span_to_test = [1.34, 5.6, -3.5, -0.3, 2.2]
        span_expected_average = 1.068
        off_average_value_to_test = 1.12
        span_code = 'test_span'
        testSymbolData.initializeSpanByCode(span_code)
        first_expected_none_value = testSymbolData.getSpanAverageByCode(span_code)
        self.assertIsNone(first_expected_none_value, 'A None value was not returned when getting the average of a span with no values')
        second_expected_none_value = testSymbolData.getPercentageDeltaOffSpanAverage(span_code, off_average_value_to_test)
        self.assertIsNone(second_expected_none_value, 'A None value was not returned when getting the percentage delta off average of a span with no values')
        for value in span_to_test:
            testSymbolData.addSpanValueByCode(span_code, value)
        span_test_average = testSymbolData.getSpanAverageByCode(span_code)
        self.assertEqual(span_expected_average, span_test_average, 'The computed average for a test span for a test SymbolData object was expected to be {0} but was {1}'.format(span_expected_average, span_test_average))
        expected_delta_off_average = 0.0486891385768
        test_delta_off_average = testSymbolData.getPercentageDeltaOffSpanAverage(span_code, off_average_value_to_test)
        test_delta_off_average = round(test_delta_off_average, 13)
        self.assertEqual(expected_delta_off_average, test_delta_off_average, 'The computed delta percentage off average for a test SymbolData object was expected to be {0} but was {1}'.format(expected_delta_off_average, test_delta_off_average))

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

    def test_getSortedDeltaValuesByCode(self):
        unsorted_dict = {'A': [1.0, 0.6], 'B': [-4.0, 0.6], 'C': [1.2, 5.7], 'D': [4.0, -8.6]}
        expected_sorted_by_delta_dict = OrderedDict()
        expected_sorted_by_delta_dict['D'] = -12.6
        expected_sorted_by_delta_dict['A'] = -0.4
        expected_sorted_by_delta_dict['C'] = 4.5
        expected_sorted_by_delta_dict['B'] = 4.6
        delta_code = 'test_sorted_deltas'
        newCollection = SymbolDataCollection()
        for symbol, delta_before_and_after_list in unsorted_dict.items():
            testSymbolData = newCollection.addSymbolToCollection(symbol)
            testSymbolData.initializeDeltaByCode(delta_code)
            testSymbolData.setDeltaBeforeByCode(delta_code, delta_before_and_after_list[0])
            testSymbolData.setDeltaAfterByCode(delta_code, delta_before_and_after_list[1])
        testNoBeforeValueSymbolData = newCollection.addSymbolToCollection('E')
        testNoBeforeValueSymbolData.initializeDeltaByCode(delta_code)
        testNoBeforeValueSymbolData.setDeltaAfterByCode(delta_code, 6.6)
        sorted_delta_values_by_code_dict = newCollection.getSortedDeltaValuesByCode(delta_code)
        self.assertEqual(expected_sorted_by_delta_dict, sorted_delta_values_by_code_dict, 'test_getSortedDeltaValuesByCode failed to return the expected sorted dictionary, returned {0}\nexpected: {1}'.format(sorted_delta_values_by_code_dict, expected_sorted_by_delta_dict))

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
        for price in first_span_prices:
            firstSymbolData.addSpanValueByCode(test_span_code, price)
        secondSymbolData = testCollection.addSymbolToCollection('B')
        secondSymbolData.initializeSpanByCode(test_span_code)
        secondSymbolData.setTodayPrice(second_today_price)
        for price in second_span_prices:
            secondSymbolData.addSpanValueByCode(test_span_code, price)
        thirdSymbolData = testCollection.addSymbolToCollection('C')
        thirdSymbolData.initializeSpanByCode(test_span_code)
        thirdSymbolData.setTodayPrice(third_today_price)
        for price in third_span_prices:
            thirdSymbolData.addSpanValueByCode(test_span_code, price)

        testNoneSymbolData = testCollection.addSymbolToCollection('D')
        testNoneSymbolData.initializeSpanByCode(test_span_code)

        test_price_off_average_values_by_symbol = testCollection.getSortedTodayPricePercentageOffSpanAveragesByCode(test_span_code)
        expected_price_off_average_values_by_symbol = OrderedDict()
        expected_price_off_average_values_by_symbol['C'] = -0.7209302325581395
        expected_price_off_average_values_by_symbol['A'] = -0.6905829596412556
        expected_price_off_average_values_by_symbol['B'] = 0.2289156626506022
        self.assertEqual(test_price_off_average_values_by_symbol, expected_price_off_average_values_by_symbol, 'The expected_price_off_average_values_by_symbol was {0}, but {1} was returned while testing getting the percentage delta of today price off of span average'.format(expected_price_off_average_values_by_symbol, test_price_off_average_values_by_symbol))


if __name__ == '__main__':
    unittest.main()