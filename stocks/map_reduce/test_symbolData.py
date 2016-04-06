import unittest
import copy
from collections import OrderedDict
from symbolData import SymbolData
from symbolData import SymbolDataCollection


class TestSymbolData(unittest.TestCase):

    def tearDown(self):
        self.unitDeltaTestsSymbol = None
        self.emptySymbol = None

    def setUp(self):
        self.unit_delta_test_symbol = 'tud'
        self.unitDeltaTestsSymbol = SymbolData(self.unit_delta_test_symbol)
        self.tud_span_code = 'tud_span'
        self.unitDeltaTestsSymbol.initializeSpanByCode(self.tud_span_code)
        self.unit_label_one = 'unit_one'
        self.unit_delta_test_list_one = [0.3, 13.4, 0.8]
        self.unit_label_two = 'unit_two'
        self.unit_delta_test_list_two = [2.8, 13.7, 0.9]
        self.unit_label_three = 'unit_three'
        self.unit_delta_test_list_three = [10.9, 14.4, 15.3]
        self.unit_label_four = 'unit_four'
        self.unit_delta_test_list_four = [15.3, 16.3, 17.3]
        self.unit_label_five = 'unit_five'
        self.unit_delta_test_list_five = [37.3, 16.3, 34.1]
        self.unit_dict = {self.unit_label_one : self.unit_delta_test_list_one,
                          self.unit_label_two : self.unit_delta_test_list_two,
                          self.unit_label_three : self.unit_delta_test_list_three,
                          self.unit_label_four : self.unit_delta_test_list_four,
                          self.unit_label_five : self.unit_delta_test_list_five}
        for label, list_of_prices in self.unit_dict.items():
            for price in list_of_prices:
                self.unitDeltaTestsSymbol.addSpanValueByCode(self.tud_span_code, [label], close_price = price, open_price = price)
        self.max_unit_delta = 4.4
        self.max_unit_delta_label = self.unit_label_three
        self.min_unit_delta = -3.2
        self.min_unit_delta_label = self.unit_label_five
        self.max_unit_delta_percentage = 1.66667
        self.max_unit_delta_percentage_label = self.unit_label_one
        self.min_unit_delta_percentage = -0.678571429
        self.min_unit_delta_percentage_label = self.unit_label_two

        self.tud_span_code_two = 'tud_span_two'
        self.tud_span_two_unit_label_one = 'span_two_unit_one'
        self.tud_span_two_unit_delta_test_list_one = [14.3, 13.4, 115.1]
        self.tud_span_two_unit_label_two = 'span_two_unit_two'
        self.tud_span_two_unit_delta_test_list_two = [32.8, 13.7, 0.9]
        self.unitDeltaTestsSymbol.initializeSpanByCode(self.tud_span_code_two)
        self.unit_dict_two = {self.tud_span_two_unit_label_one : self.tud_span_two_unit_delta_test_list_one,
                              self.tud_span_two_unit_label_two : self.tud_span_two_unit_delta_test_list_two}
        for label, list_of_prices in self.unit_dict_two.items():
            for price in list_of_prices:
                self.unitDeltaTestsSymbol.addSpanValueByCode(self.tud_span_code_two, [label], close_price = price, open_price = price)
        self.max_unit_delta_two = 100.8
        self.max_unit_delta_percentage_two = 7.048951049
        self.max_unit_delta_label_two = self.tud_span_two_unit_label_one
        self.min_unit_delta_two = -31.9
        self.min_unit_delta_percentage_two = -0.972560976
        self.min_unit_delta_label_two = self.tud_span_two_unit_label_two
        self.expected_span_delta_value_to_span_max_delta_ratio = -3.045454545
        self.expected_span_delta_value_to_span_max_delta_percentage_ratio = -0.56224
        self.expected_span_delta_value_to_span_min_delta_ratio = 4.1875
        self.expected_span_delta_value_to_span_min_delta_percentage_ratio = 1.38093

        self.empty_symbol_code = 'empty'
        self.emptySymbol = SymbolData(self.empty_symbol_code)
        self.emptySymbol.initializeSpanByCode(self.tud_span_code)

        self.zero_delta_span_code = 'zero_delta'
        self.zero_delta_span = [1.34, 5.6, -3.5, -0.3, 1.34]
        self.zero_delta_span_off_average_test_value = 1.56
        self.unitDeltaTestsSymbol.initializeSpanByCode(self.zero_delta_span_code)
        for value in self.zero_delta_span:
            self.unitDeltaTestsSymbol.addSpanValueByCode(self.zero_delta_span_code, self.zero_delta_span_code, close_price = value, open_price = value)
        self.expected_percentage_delta_off_zero_delta_span_average = 0.7410714285714288

        self.zero_average_span_code = 'zero_average'
        self.zero_average_span = [1.34, 0.45, -1.65, -1.54, 1.4]
        self.unitDeltaTestsSymbol.initializeSpanByCode(self.zero_average_span_code)
        for value in self.zero_average_span:
            self.unitDeltaTestsSymbol.addSpanValueByCode(self.zero_average_span_code, self.zero_average_span_code, close_price = value, open_price = value)
        self.expected_percentage_delta_off_zero_delta_span_average = 0.7410714285714288

        self.span_unit_deltas = [-1.3, -1.5, .3, 4.4, 5.0, 1.1]
        self.expected_span_unit_min_delta = -1.5
        self.span_unit_delta_percentages = [-.13, -.15, .3, .4, -.04, 2.3]
        self.expected_unit_delta_percentage = -.15
        self.span_code_prefix = 'span_unit_'
        self.spanUnitDeltasSymbol = SymbolData('span_unit_deltas')
        for i in range(0, len(self.span_unit_deltas)):
            unit_delta = self.span_unit_deltas[i]
            unit_delta_percentage = self.span_unit_delta_percentages[i]
            span_code = self.span_code_prefix + str(i)
            self.spanUnitDeltasSymbol.addSpanValueByCode(span_code, [span_code], delta = unit_delta, delta_percentage = unit_delta_percentage)
            oscillating_span_code = self.span_code_prefix + str(i % 2)
            self.spanUnitDeltasSymbol.addSpanValueByCode(oscillating_span_code, [oscillating_span_code], delta = unit_delta, delta_percentage = unit_delta_percentage)

    def test_getMinSpanUnitDelta(self):
        test_min_unit_delta = self.spanUnitDeltasSymbol.getMinSpanUnitDelta()
        self.assertEqual(self.expected_span_unit_min_delta, test_min_unit_delta)
        test_min_unit_delta_percentage = self.spanUnitDeltasSymbol.getMinSpanUnitDelta(get_percentage_delta = True)
        self.assertEqual(test_min_unit_delta_percentage, self.expected_unit_delta_percentage)

    def test_getSpanByCode(self):
        zero_delta_span = self.unitDeltaTestsSymbol.getSpanByCode(self.zero_delta_span_code)
        self.assertEqual(self.zero_delta_span_code, zero_delta_span.code)
        self.assertEqual(zero_delta_span.getUnitsCount(), 5)
        tudSpan = self.unitDeltaTestsSymbol.getSpanByCode(self.tud_span_code)
        self.assertEqual(self.tud_span_code, tudSpan.code)
        self.assertEqual(15, tudSpan.getUnitsCount())
        tudSpanTwo = self.unitDeltaTestsSymbol.getSpanByCode(self.tud_span_code_two)
        self.assertEqual(self.tud_span_code_two, tudSpanTwo.code)
        self.assertEqual(6, tudSpanTwo.getUnitsCount())

    def test_getSpanUnitCount(self):
        self.assertEqual(self.unitDeltaTestsSymbol.getSpanUnitCount(self.zero_delta_span_code), 5)
        self.assertEqual(self.unitDeltaTestsSymbol.getSpanUnitCount(self.zero_average_span_code), 5)
        self.assertEqual(self.unitDeltaTestsSymbol.getSpanUnitCount(self.tud_span_code), 15)
        self.assertEqual(self.unitDeltaTestsSymbol.getSpanUnitCount(self.tud_span_code_two), 6)
        self.assertEqual(self.emptySymbol.getSpanUnitCount(self.tud_span_code), 0)

    def test_getPercentageDeltaOffSpanAverage(self):
        test_percentage_delta_off_zero_delta_span_average = self.unitDeltaTestsSymbol.getPercentageDeltaOffSpanAverage(self.zero_delta_span_code, self.zero_delta_span_off_average_test_value)
        self.assertEqual(test_percentage_delta_off_zero_delta_span_average, self.expected_percentage_delta_off_zero_delta_span_average)
        test_percentage_delta_off_zero_average_span_average = self.unitDeltaTestsSymbol.getPercentageDeltaOffSpanAverage(self.zero_average_span_code, self.zero_delta_span_off_average_test_value)
        self.assertEqual(test_percentage_delta_off_zero_average_span_average, float("inf"))

    def test_getSpanDeltaValueToSpanMaxDeltaRatio(self):
        test_max_ratio = round(self.unitDeltaTestsSymbol.getSpanDeltaValueToSpanMaxDeltaRatio(self.tud_span_code_two, self.tud_span_code), 9)
        self.assertEqual(test_max_ratio, self.expected_span_delta_value_to_span_max_delta_ratio)
        test_max_ratio_percentage = round(self.unitDeltaTestsSymbol.getSpanDeltaValueToSpanMaxDeltaRatio(self.tud_span_code_two, self.tud_span_code, True), 5)
        self.assertEqual(test_max_ratio_percentage, self.expected_span_delta_value_to_span_max_delta_percentage_ratio)
        test_percentage_delta_off_zero_delta_span_average = self.unitDeltaTestsSymbol.getSpanDeltaValueToSpanMaxDeltaRatio(self.tud_span_code_two, self.zero_delta_span_code)
        self.assertEqual(test_percentage_delta_off_zero_delta_span_average, float("inf"))

    def test_getSpanDeltaValueToSpanMinDeltaRatio(self):
        test_min_ratio = round(self.unitDeltaTestsSymbol.getSpanDeltaValueToSpanMinDeltaRatio(self.tud_span_code_two, self.tud_span_code), 4)
        self.assertEqual(test_min_ratio, self.expected_span_delta_value_to_span_min_delta_ratio)
        test_min_ratio_percentage = round(self.unitDeltaTestsSymbol.getSpanDeltaValueToSpanMinDeltaRatio(self.tud_span_code_two, self.tud_span_code, True), 5)
        self.assertEqual(test_min_ratio_percentage, self.expected_span_delta_value_to_span_min_delta_percentage_ratio)
        test_percentage_delta_off_zero_delta_span_average = self.unitDeltaTestsSymbol.getSpanDeltaValueToSpanMinDeltaRatio(self.tud_span_code_two, self.zero_delta_span_code)
        self.assertEqual(test_percentage_delta_off_zero_delta_span_average, float("inf"))

    def test_getMaxDeltaForSpan(self):
        test_max_delta_for_span_tud_one = self.unitDeltaTestsSymbol.getMaxDeltaForSpan(self.tud_span_code)
        test_max_value = test_max_delta_for_span_tud_one['delta']
        test_max_value_label = test_max_delta_for_span_tud_one['label']
        self.assertEqual(test_max_value, self.max_unit_delta)
        self.assertEqual(test_max_value_label, self.max_unit_delta_label)
        test_max_delta_percentage = self.unitDeltaTestsSymbol.getMaxDeltaForSpan(self.tud_span_code, True)
        test_max_delta_percentage_value = round(test_max_delta_percentage['delta'], 5)
        test_max_delta_percentage_label = test_max_delta_percentage['label']
        self.assertEqual(test_max_delta_percentage_value, self.max_unit_delta_percentage)
        self.assertEqual(test_max_delta_percentage_label, self.max_unit_delta_percentage_label)
        test_max_delta_two = self.unitDeltaTestsSymbol.getMaxDeltaForSpan(self.tud_span_code_two)
        test_max_value_two = test_max_delta_two['delta']
        test_max_value_label_two = test_max_delta_two['label']
        self.assertEqual(test_max_value_two, self.max_unit_delta_two)
        self.assertEqual(test_max_value_label_two, self.max_unit_delta_label_two)
        test_max_delta_percentage_two = self.unitDeltaTestsSymbol.getMaxDeltaForSpan(self.tud_span_code_two, True)
        test_max_delta_percentage_value_two = round(test_max_delta_percentage_two['delta'], 9)
        test_max_delta_percentage_label_two = test_max_delta_percentage_two['label']
        self.assertEqual(test_max_delta_percentage_value_two, self.max_unit_delta_percentage_two)
        self.assertEqual(test_max_delta_percentage_label_two, self.max_unit_delta_label_two)
        # Test empty Symbol
        test_empty_value = self.emptySymbol.getMaxDeltaForSpan(self.tud_span_code)
        test_value = test_empty_value['delta']
        test_label = test_empty_value['label']
        self.assertIsNone(test_value)
        self.assertIsNone(test_label)

    def test_getMaxDeltaForSpanValue(self):
        test_max_value_explicit = self.unitDeltaTestsSymbol.getMaxDeltaForSpanValue(self.tud_span_code)
        self.assertEqual(test_max_value_explicit, self.max_unit_delta)
        test_max_delta_percentage_value_explicit = round(self.unitDeltaTestsSymbol.getMaxDeltaForSpanValue(self.tud_span_code, True), 5)
        self.assertEqual(test_max_delta_percentage_value_explicit, self.max_unit_delta_percentage)

    def test_getMinDeltaForSpan(self):
        test_min_delta_for_span_tud_one = self.unitDeltaTestsSymbol.getMinDeltaForSpan(self.tud_span_code)
        test_min_value = round(test_min_delta_for_span_tud_one['delta'], 2)
        test_min_value_label = test_min_delta_for_span_tud_one['label']
        self.assertEqual(test_min_value, self.min_unit_delta)
        self.assertEqual(test_min_value_label, self.min_unit_delta_label)
        test_min_delta_percentage = self.unitDeltaTestsSymbol.getMinDeltaForSpan(self.tud_span_code, True)
        test_min_delta_percentage_value = round(test_min_delta_percentage['delta'], 9)
        test_min_delta_percentage_label = test_min_delta_percentage['label']
        self.assertEqual(test_min_delta_percentage_value, self.min_unit_delta_percentage)
        self.assertEqual(test_min_delta_percentage_label, self.min_unit_delta_percentage_label)
        test_min_delta_two = self.unitDeltaTestsSymbol.getMinDeltaForSpan(self.tud_span_code_two)
        test_min_value_two = round(test_min_delta_two['delta'], 2)
        test_min_value_label_two = test_min_delta_two['label']
        self.assertEqual(test_min_value_two, self.min_unit_delta_two)
        self.assertEqual(test_min_value_label_two, self.min_unit_delta_label_two)
        test_min_delta_percentage_two = self.unitDeltaTestsSymbol.getMinDeltaForSpan(self.tud_span_code_two, True)
        test_min_delta_percentage_value_two = round(test_min_delta_percentage_two['delta'], 9)
        test_min_delta_percentage_label_two = test_min_delta_percentage_two['label']
        self.assertEqual(test_min_delta_percentage_value_two, self.min_unit_delta_percentage_two)
        self.assertEqual(test_min_delta_percentage_label_two, self.min_unit_delta_label_two)
        # Test empty Symbol
        test_empty_value = self.emptySymbol.getMinDeltaForSpan(self.tud_span_code)
        test_value = test_empty_value['delta']
        test_label = test_empty_value['label']
        self.assertIsNone(test_value)
        self.assertIsNone(test_label)

    def test_getMinDeltaForSpanValue(self):
        test_min_value_explicit = round(self.unitDeltaTestsSymbol.getMinDeltaForSpanValue(self.tud_span_code), 2)
        self.assertEqual(test_min_value_explicit, self.min_unit_delta)
        test_min_delta_percentage_explicit = round(self.unitDeltaTestsSymbol.getMinDeltaForSpanValue(self.tud_span_code, True), 9)
        self.assertEqual(test_min_delta_percentage_explicit, self.min_unit_delta_percentage)

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
        expected_span_delta_percentage = round(expected_span_delta / 1.10, 6)
        test_span_delta_percentage = round(testSymbolData.getSpanDeltaByCode(span_code, get_percentage = True), 6)
        self.assertEqual(expected_span_delta_percentage, test_span_delta_percentage)
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
        test_even_units_delta_percentage = round(testSymbolData.getSpanDeltaByCode(span_code, even_units_label, get_percentage = True), 6)
        expected_even_units_delta_percentage = round(expected_even_units_delta / 1.10, 6)
        self.assertEqual(expected_even_units_delta_percentage, test_even_units_delta_percentage)
        test_odd_units_delta_percentage = round(testSymbolData.getSpanDeltaByCode(span_code, odd_units_label, get_percentage = True), 6)
        expected_even_units_delta_percentage = round(expected_odd_units_delta / 1.0, 6)
        self.assertEqual(expected_even_units_delta_percentage, test_odd_units_delta_percentage)

class TestSymbolDataCollection(unittest.TestCase):

    def tearDown(self):
        self.unitDeltaTestsSymbol = None

    def setUp(self):
        self.get_symbol_span_value_vectors_all_length_list_dict = {}

        self.unitDeltaTestsSymbolCollection = SymbolDataCollection()
        self.unit_delta_test_symbol = 'tud'
        self.unitDeltaTestsSymbolOne = self.unitDeltaTestsSymbolCollection.addSymbolToCollection(self.unit_delta_test_symbol)
        self.tud_span_code = 'tud_span'
        self.unitDeltaTestsSymbolOne.initializeSpanByCode(self.tud_span_code)
        self.unit_label_one = 'unit_one'
        self.unit_delta_test_list_one = [0.3, 13.4, 0.8]
        self.unit_label_two = 'unit_two'
        self.unit_delta_test_list_two = [2.8, 13.7, 0.9]
        self.unit_label_three = 'unit_three'
        self.unit_delta_test_list_three = [10.9, 14.4, 15.3]
        self.unit_label_four = 'unit_four'
        self.unit_delta_test_list_four = [15.3, 16.3, 17.3]
        self.unit_label_five = 'unit_five'
        self.unit_delta_test_list_five = [37.3, 16.3, 34.1]
        self.unit_dict = {self.unit_label_one : self.unit_delta_test_list_one,
                          self.unit_label_two : self.unit_delta_test_list_two,
                          self.unit_label_three : self.unit_delta_test_list_three,
                          self.unit_label_four : self.unit_delta_test_list_four,
                          self.unit_label_five : self.unit_delta_test_list_five}
        self.get_symbol_span_value_vectors_all_length_list_dict[self.unit_delta_test_symbol] = list()
        for label, list_of_prices in self.unit_dict.items():
            for price in list_of_prices:
                self.unitDeltaTestsSymbolOne.addSpanValueByCode(self.tud_span_code, [label], close_price = price, open_price = price)
                self.get_symbol_span_value_vectors_all_length_list_dict[self.unit_delta_test_symbol].append(price)
        self.tud_span_code_two = 'tud_span_two'
        self.tud_span_two_unit_label_one = 'span_two_unit_one'
        self.tud_span_two_unit_delta_test_list_one = [14.3, 13.4, 115.1]
        self.tud_span_two_unit_label_two = 'span_two_unit_two'
        self.tud_span_two_unit_delta_test_list_two = [32.8, 13.7, 0.9]
        self.unitDeltaTestsSymbolOne.initializeSpanByCode(self.tud_span_code_two)
        self.unit_dict_two = {self.tud_span_two_unit_label_one : self.tud_span_two_unit_delta_test_list_one,
                              self.tud_span_two_unit_label_two : self.tud_span_two_unit_delta_test_list_two}
        for label, list_of_prices in self.unit_dict_two.items():
            for price in list_of_prices:
                self.unitDeltaTestsSymbolOne.addSpanValueByCode(self.tud_span_code_two, [label], close_price = price, open_price = price)

        self.unit_delta_test_symbol_two = 'tud_two'
        self.unitDeltaTestsSymbolTwo = self.unitDeltaTestsSymbolCollection.addSymbolToCollection(self.unit_delta_test_symbol_two)
        self.unitDeltaTestsSymbolTwo.initializeSpanByCode(self.tud_span_code)
        self.unitDeltaTestsSymbolTwo.initializeSpanByCode(self.tud_span_code_two)
        self.unit_delta_test_list_one_two = [2.5, 6.7, 4.5]
        self.unit_delta_test_list_two_two = [6.7, 7.6, 6.9]
        self.unit_delta_test_list_three_two = [7.6, 7.8, 6.6]
        self.unit_delta_test_list_four_two = [12.4, 23.4, 6.6]
        self.unit_delta_test_list_five_two = [0.2, 0.5, 0.9]
        self.unit_dict_two = {self.unit_label_one : self.unit_delta_test_list_one_two,
                          self.unit_label_two : self.unit_delta_test_list_two_two,
                          self.unit_label_three : self.unit_delta_test_list_three_two,
                          self.unit_label_four : self.unit_delta_test_list_four_two,
                          self.unit_label_five : self.unit_delta_test_list_five_two}
        self.get_symbol_span_value_vectors_all_length_list_dict[self.unit_delta_test_symbol_two] = list()
        for label, list_of_prices in self.unit_dict_two.items():
            for price in list_of_prices:
                self.unitDeltaTestsSymbolTwo.addSpanValueByCode(self.tud_span_code, [label], close_price = price, open_price = price)
                self.get_symbol_span_value_vectors_all_length_list_dict[self.unit_delta_test_symbol_two].append(price)

        self.tud_span_two_unit_delta_test_list_one_two = [10.5, 8.5, 5.4]
        self.tud_span_two_unit_delta_test_list_two_two = [5.5, 6.5, 11.9]
        self.unit_dict_two_two = {self.tud_span_two_unit_label_one : self.tud_span_two_unit_delta_test_list_one_two,
                                    self.tud_span_two_unit_label_two : self.tud_span_two_unit_delta_test_list_two_two}
        for label, list_of_prices in self.unit_dict_two_two.items():
            for price in list_of_prices:
                self.unitDeltaTestsSymbolTwo.addSpanValueByCode(self.tud_span_code_two, [label], close_price = price, open_price = price)

        self.unit_delta_test_symbol_three = 'tud_three'
        self.unitDeltaTestsSymbolThree = self.unitDeltaTestsSymbolCollection.addSymbolToCollection(self.unit_delta_test_symbol_three)
        self.unitDeltaTestsSymbolThree.initializeSpanByCode(self.tud_span_code)
        self.unitDeltaTestsSymbolThree.initializeSpanByCode(self.tud_span_code_two)
        self.unit_delta_test_list_one_three = [12.4, 15.6, 14.5]
        self.unit_delta_test_list_two_three = [16.7, 17.6, 16.9]
        self.unit_delta_test_list_three_three = [10.1, 11.3, 12.5]
        self.unit_delta_test_list_four_three = [5.6, 7.8, 5.5]
        self.unit_delta_test_list_five_three = [3.4, 4.5, 4.0]
        self.unit_dict_three = {self.unit_label_one : self.unit_delta_test_list_one_three,
                              self.unit_label_two : self.unit_delta_test_list_two_three,
                              self.unit_label_three : self.unit_delta_test_list_three_three,
                              self.unit_label_four : self.unit_delta_test_list_four_three,
                              self.unit_label_five : self.unit_delta_test_list_five_three}
        self.get_symbol_span_value_vectors_all_length_list_dict[self.unit_delta_test_symbol_three] = list()
        for label, list_of_prices in self.unit_dict_three.items():
            for price in list_of_prices:
                self.unitDeltaTestsSymbolThree.addSpanValueByCode(self.tud_span_code, [label], close_price = price, open_price = price)
                self.get_symbol_span_value_vectors_all_length_list_dict[self.unit_delta_test_symbol_three].append(price)

        self.tud_span_two_unit_delta_test_list_one_three = [8.8, 8.9, 9.4]
        self.tud_span_two_unit_delta_test_list_two_three = [4.5, 5.5, 4.0]
        self.unit_dict_two_three = {self.tud_span_two_unit_label_one : self.tud_span_two_unit_delta_test_list_one_three,
                                    self.tud_span_two_unit_label_two : self.tud_span_two_unit_delta_test_list_two_three}
        for label, list_of_prices in self.unit_dict_two_three.items():
            for price in list_of_prices:
                self.unitDeltaTestsSymbolThree.addSpanValueByCode(self.tud_span_code_two, [label], close_price = price, open_price = price)
        self.expected_sorted_max_span_delta_ratio = OrderedDict()
        self.expected_sorted_max_span_delta_ratio[self.unit_delta_test_symbol] = -3.0454545454545454
        self.expected_sorted_max_span_delta_ratio[self.unit_delta_test_symbol_three] = -2
        self.expected_sorted_max_span_delta_ratio[self.unit_delta_test_symbol_two] = 0.7000000000000002
        self.expected_sorted_max_span_delta_percentage_ratio = OrderedDict()
        self.expected_sorted_max_span_delta_percentage_ratio[self.unit_delta_test_symbol_three] = -2.2954545454545454
        self.expected_sorted_max_span_delta_percentage_ratio[self.unit_delta_test_symbol] = -0.5622377622377622
        self.expected_sorted_max_span_delta_percentage_ratio[self.unit_delta_test_symbol_two] = 0.038095238095238106
        self.expected_sorted_min_span_delta_ratio = OrderedDict()
        self.expected_sorted_min_span_delta_ratio[self.unit_delta_test_symbol_two] = -0.24137931034482762
        self.expected_sorted_min_span_delta_ratio[self.unit_delta_test_symbol] = 4.187500000000005
        self.expected_sorted_min_span_delta_ratio[self.unit_delta_test_symbol_three] = 48.00000000000018
        self.expected_sorted_min_span_delta_percentage_ratio = OrderedDict()
        self.expected_sorted_min_span_delta_percentage_ratio[self.unit_delta_test_symbol_two] = -0.28505747126436787
        self.expected_sorted_min_span_delta_percentage_ratio[self.unit_delta_test_symbol] = 1.3809348546190652
        self.expected_sorted_min_span_delta_percentage_ratio[self.unit_delta_test_symbol_three] = 30.545454545454657

        self.symbol_span_value_vectors_test_span = 'symbol_span_value_vectors_test'
        self.spanValueVectorsTestSymbol = self.unitDeltaTestsSymbolCollection.addSymbolToCollection(self.symbol_span_value_vectors_test_span)
        self.spanValueVectorsTestSymbol.initializeSpanByCode(self.tud_span_code)
        self.spanValueVectorsTestSymbol.initializeSpanByCode(self.tud_span_code_two)
        self.symbol_span_value_vectors_test_open_prices = [1.0, 2.0, 3.0, 4.0]
        self.symbol_span_value_vectors_test_close_prices = [5.0, 6.0, 7.0]

        self.get_symbol_span_value_vectors_open_price_all_length_list_dict = copy.deepcopy(self.get_symbol_span_value_vectors_all_length_list_dict)
        self.get_symbol_span_value_vectors_close_price_all_length_list_dict = copy.deepcopy(self.get_symbol_span_value_vectors_all_length_list_dict)
        self.get_symbol_span_value_vectors_open_price_max_length_list_dict = copy.deepcopy(self.get_symbol_span_value_vectors_all_length_list_dict)
        self.get_symbol_span_value_vectors_close_price_max_length_list_dict = copy.deepcopy(self.get_symbol_span_value_vectors_all_length_list_dict)

        self.get_symbol_span_value_vectors_open_price_all_length_list_dict[self.symbol_span_value_vectors_test_span] = list()
        self.get_symbol_span_value_vectors_close_price_all_length_list_dict[self.symbol_span_value_vectors_test_span] = list()

        for price in self.symbol_span_value_vectors_test_open_prices:
            self.spanValueVectorsTestSymbol.addSpanValueByCode(self.tud_span_code, [], open_price = price)
            self.get_symbol_span_value_vectors_open_price_all_length_list_dict[self.symbol_span_value_vectors_test_span].append(price)

        for price in self.symbol_span_value_vectors_test_close_prices:
            self.spanValueVectorsTestSymbol.addSpanValueByCode(self.tud_span_code, [], close_price = price)
            self.get_symbol_span_value_vectors_close_price_all_length_list_dict[self.symbol_span_value_vectors_test_span].append(price)

    def test_getSymbolSpanValueVectors(self):
        test_all_open_price_values_list_dict = self.unitDeltaTestsSymbolCollection.getSymbolSpanValueVectors(self.tud_span_code, 'open_price', only_vectors_at_max_length = False)
        test_all_open_price_values_list_value = test_all_open_price_values_list_dict['value_vectors']
        test_all_open_price_values_list = test_all_open_price_values_list_dict['symbols_list']
        index = 0
        for symbol in test_all_open_price_values_list:
            test_value_vector = test_all_open_price_values_list_value[index]
            expected_value_vector = self.get_symbol_span_value_vectors_open_price_all_length_list_dict[symbol]
            self.assertEqual(test_value_vector, expected_value_vector)
            index += 1

        test_all_close_price_values_list_dict = self.unitDeltaTestsSymbolCollection.getSymbolSpanValueVectors(self.tud_span_code, 'close_price', only_vectors_at_max_length = False)
        test_all_close_price_values_list_value = test_all_close_price_values_list_dict['value_vectors']
        test_all_close_price_values_list = test_all_close_price_values_list_dict['symbols_list']
        index = 0
        for symbol in test_all_close_price_values_list:
            test_value_vector = test_all_close_price_values_list_value[index]
            expected_value_vector = self.get_symbol_span_value_vectors_close_price_all_length_list_dict[symbol]
            self.assertEqual(test_value_vector, expected_value_vector)
            index += 1

        test_max_open_price_values_list_dict = self.unitDeltaTestsSymbolCollection.getSymbolSpanValueVectors(self.tud_span_code, 'open_price', only_vectors_at_max_length = True)
        test_max_open_price_values_list_value = test_max_open_price_values_list_dict['value_vectors']
        test_max_open_price_values_list = test_max_open_price_values_list_dict['symbols_list']
        index = 0
        for symbol in test_max_open_price_values_list:
            test_value_vector = test_max_open_price_values_list_value[index]
            expected_value_vector = self.get_symbol_span_value_vectors_open_price_max_length_list_dict[symbol]
            self.assertEqual(test_value_vector, expected_value_vector)
            index += 1

        test_max_close_price_values_list_dict = self.unitDeltaTestsSymbolCollection.getSymbolSpanValueVectors(self.tud_span_code, 'close_price', only_vectors_at_max_length = True)
        test_max_close_price_values_list_value = test_max_close_price_values_list_dict['value_vectors']
        test_max_close_price_values_list = test_max_close_price_values_list_dict['symbols_list']
        index = 0
        for symbol in test_max_close_price_values_list:
            test_value_vector = test_max_close_price_values_list_value[index]
            expected_value_vector = self.get_symbol_span_value_vectors_close_price_max_length_list_dict[symbol]
            self.assertEqual(test_value_vector, expected_value_vector)
            index += 1

    def test_getSortedSpanDeltaValueToSpanMaxDeltaRatios(self):
        test_sorted_max_span_delta_ratio = self.unitDeltaTestsSymbolCollection.getSortedSpanDeltaValueToSpanMaxDeltaRatios(self.tud_span_code_two, self.tud_span_code)
        self.assertEqual(test_sorted_max_span_delta_ratio, self.expected_sorted_max_span_delta_ratio)
        test_sorted_max_span_delta_percentage_ratio = self.unitDeltaTestsSymbolCollection.getSortedSpanDeltaValueToSpanMaxDeltaRatios(self.tud_span_code_two, self.tud_span_code, get_percentage = True)
        self.assertEqual(test_sorted_max_span_delta_percentage_ratio, self.expected_sorted_max_span_delta_percentage_ratio)

    def test_getSortedSpanDeltaValueToSpanMinDeltaRatios(self):
        test_sorted_min_span_delta_ratio = self.unitDeltaTestsSymbolCollection.getSortedSpanDeltaValueToSpanMinDeltaRatios(self.tud_span_code_two, self.tud_span_code)
        self.assertEqual(test_sorted_min_span_delta_ratio, self.expected_sorted_min_span_delta_ratio)
        test_sorted_min_span_delta_percentage_ratio = self.unitDeltaTestsSymbolCollection.getSortedSpanDeltaValueToSpanMinDeltaRatios(self.tud_span_code_two, self.tud_span_code, get_percentage = True)
        self.assertEqual(test_sorted_min_span_delta_percentage_ratio, self.expected_sorted_min_span_delta_percentage_ratio)

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
        expected_sorted_by_delta_percentages_dict = OrderedDict()
        expected_sorted_by_delta_percentages_dict['B'] = -15.166666666666666
        expected_sorted_by_delta_percentages_dict['C'] = -0.6140350877192983
        expected_sorted_by_delta_percentages_dict['A'] = 1.5
        expected_sorted_by_delta_percentages_dict['D'] = 1.5116279069767442
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
        # Test delta percentages
        sorted_span_delta_percentage_values_by_code_dict = newCollection.getSortedSpanDeltaValuesByCode(span_code, 'test_unit_code', get_percentages = True)
        self.assertEqual(expected_sorted_by_delta_percentages_dict, sorted_span_delta_percentage_values_by_code_dict)

    def test_getSortedTodayPricePercentageOffSpanAveragesByCode(self):
        test_span_code = 'percentage_price_off_span_average_test'
        testCollection = SymbolDataCollection()
        first_today_price = 2.3
        first_span_prices = [5.7, 8.9, 7.7]
        second_today_price = 3.4
        second_span_prices = [2.3, 2.7, 3.3]
        third_today_price = 1.2
        third_span_prices = [4.6, 2.7, 5.6]
        fourth_today_price = 3.4
        fourth_span_prices = [-2.0, 0.0, 2.0]
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
        testNoneSymbolData.addSpanValueByCode(test_span_code, 'irrelevant_label', 12.3)
        testNoneSymbolData.setTodayPrice(13.3)

        test_price_off_average_values_by_symbol = testCollection.getSortedTodayPricePercentageOffSpanAveragesByCode(test_span_code)
        expected_price_off_average_values_by_symbol = OrderedDict()
        expected_price_off_average_values_by_symbol['C'] = -0.7209302325581395
        expected_price_off_average_values_by_symbol['A'] = -0.6905829596412556
        expected_price_off_average_values_by_symbol['D'] = 0.08130081300813008
        expected_price_off_average_values_by_symbol['B'] = 0.2289156626506022
        self.assertEqual(test_price_off_average_values_by_symbol, expected_price_off_average_values_by_symbol, 'The expected_price_off_average_values_by_symbol was {0}, but {1} was returned while testing getting the percentage delta of today price off of span average'.format(expected_price_off_average_values_by_symbol, test_price_off_average_values_by_symbol))
        price_threshold = 2.0
        test_sorted_dictionary_of_values_above_price_threshold = testCollection.getSortedDictionaryOfValuesAboveTodayPriceThreshold(test_price_off_average_values_by_symbol, price_threshold)
        expected_price_off_average_values_by_symbol_above_threshold = OrderedDict()
        expected_price_off_average_values_by_symbol_above_threshold['A'] = -0.6905829596412556
        expected_price_off_average_values_by_symbol_above_threshold['D'] = 0.08130081300813008
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