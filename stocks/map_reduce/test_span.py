import unittest

from span import Span


class TestSpan(unittest.TestCase):

    def tearDown(self):
        self.unitDeltaTestsSpan = None
        self.testSpan = None

    def setUp(self):
        self.empty_span_code = 'empty_span'
        self.emptySpan = Span(self.empty_span_code)

        self.span_code = 'test_span'
        self.testSpan = Span(self.span_code)
        self.unitDeltaTestsSpan = Span('unit_delta_tests_span')
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
                self.unitDeltaTestsSpan.addSpanUnit([label], close_price = price, open_price = price)
        self.max_unit_delta = 4.4
        self.max_unit_delta_label = self.unit_label_three
        self.min_unit_delta = -3.2
        self.min_unit_delta_label = self.unit_label_five
        self.max_unit_delta_percentage = 1.66667
        self.max_unit_delta_percentage_label = self.unit_label_one
        self.min_unit_delta_percentage = -0.678571429
        self.min_unit_delta_percentage_label = self.unit_label_two

        self.zero_tests_span_code = 'zero_tests'
        self.zeroTestsSpan =  Span(self.zero_tests_span_code)
        self.zero_delta_span_code = 'zero_delta'
        self.zero_delta_span_list = [0.0, 3.1, 4.4, 0.0]
        for value in self.zero_delta_span_list:
            self.zeroTestsSpan.addSpanUnit([self.zero_delta_span_code], value, value)
        self.zero_delta_non_zero_close_span_code = 'zero_delta_non_zero_close'
        self.zero_delta_non_zero_close_span_list = [0.0, 3.1, 4.4, 2.0]
        for value in self.zero_delta_non_zero_close_span_list:
            self.zeroTestsSpan.addSpanUnit([self.zero_delta_non_zero_close_span_code], value, value)
        self.zero_average_test_span_code = 'zero_span_close_average'
        self.zero_average_test_span_list = [0.0, -1.0, -1.0, 2.0]
        for value in self.zero_average_test_span_list:
            self.zeroTestsSpan.addSpanUnit([self.zero_average_test_span_code], value, value)

    def test_getUnitsCount(self):
        self.assertEqual(self.zeroTestsSpan.getUnitsCount(), 12)
        self.assertEqual(self.emptySpan.getUnitsCount(), 0)
        self.assertEqual(self.unitDeltaTestsSpan.getUnitsCount(), 15)

    def test_getSpanDelta(self):
        test_zero_delta_span_delta = self.zeroTestsSpan.getSpanDelta(self.zero_delta_span_code)
        self.assertEqual(test_zero_delta_span_delta, 0.0)
        test_zero_delta_span_delta_percentage = self.zeroTestsSpan.getSpanDelta(self.zero_delta_span_code, get_percentage_delta = True)
        self.assertEqual(test_zero_delta_span_delta_percentage, 0.0)
        test_zero_delta_non_zero_close_span_delta = self.zeroTestsSpan.getSpanDelta(self.zero_delta_non_zero_close_span_code)
        self.assertEqual(test_zero_delta_non_zero_close_span_delta, 2.0)
        test_zero_delta_non_zero_close_span_delta_percentage = self.zeroTestsSpan.getSpanDelta(self.zero_delta_non_zero_close_span_code, get_percentage_delta = True)
        self.assertEqual(test_zero_delta_non_zero_close_span_delta_percentage, float("inf"))

    def test_getMaxUnitDelta(self):
        test_max_unit_delta = self.unitDeltaTestsSpan.getMaxUnitDelta()
        test_max_unit_delta_value = test_max_unit_delta['delta']
        test_max_unit_delta_value_label = test_max_unit_delta['label']
        self.assertEqual(test_max_unit_delta_value, self.max_unit_delta)
        self.assertEqual(test_max_unit_delta_value_label, self.max_unit_delta_label)
        test_max_unit_delta_percentage = self.unitDeltaTestsSpan.getMaxUnitDelta(True)
        test_max_unit_delta_percentage_value = round(test_max_unit_delta_percentage['delta'], 5)
        test_max_unit_delta_percentage_value_label = test_max_unit_delta_percentage['label']
        self.assertEqual(test_max_unit_delta_percentage_value, self.max_unit_delta_percentage)
        self.assertEqual(test_max_unit_delta_percentage_value_label, self.max_unit_delta_percentage_label)

    def test_getMaxUnitDeltaValue(self):
        test_max_unit_delta_value_explicit = self.unitDeltaTestsSpan.getMaxUnitDeltaValue()
        self.assertEqual(test_max_unit_delta_value_explicit, self.max_unit_delta)
        test_max_unit_delta_percentage_value_explicit = round(self.unitDeltaTestsSpan.getMaxUnitDeltaValue(True), 5)
        self.assertEqual(test_max_unit_delta_percentage_value_explicit, self.max_unit_delta_percentage)

    def test_getMinUnitDelta(self):
        test_min_unit_delta = self.unitDeltaTestsSpan.getMinUnitDelta()
        test_min_unit_delta_value = round(test_min_unit_delta['delta'], 2)
        test_min_unit_delta_value_label = test_min_unit_delta['label']
        self.assertEqual(test_min_unit_delta_value, self.min_unit_delta)
        self.assertEqual(test_min_unit_delta_value_label, self.min_unit_delta_label)
        test_min_unit_delta_percentage = self.unitDeltaTestsSpan.getMinUnitDelta(True)
        test_min_unit_delta_percentage_value = round(test_min_unit_delta_percentage['delta'], 9)
        test_min_unit_delta_percentage_value_label = test_min_unit_delta_percentage['label']
        self.assertEqual(test_min_unit_delta_percentage_value, self.min_unit_delta_percentage)
        self.assertEqual(test_min_unit_delta_percentage_value_label, self.min_unit_delta_percentage_label)

    def test_getMinUnitDeltaValue(self):
        test_min_unit_delta_value_explicit = round(self.unitDeltaTestsSpan.getMinUnitDeltaValue(), 2)
        self.assertEqual(test_min_unit_delta_value_explicit, self.min_unit_delta)
        test_min_unit_delta_percentage_value_explicit = round(self.unitDeltaTestsSpan.getMinUnitDeltaValue(True), 9)
        self.assertEqual(test_min_unit_delta_percentage_value_explicit, self.min_unit_delta_percentage)

    def test_spanInit(self):
        self.assertEquals(self.span_code, self.testSpan.code, 'The code returned from a Span object does not match the code used to initialize the object')

    def test_addSpanUnit(self):
        span_delta = self.emptySpan.getSpanDelta()
        self.assertIsNone(span_delta)
        first_unit_label = 'first_unit'
        second_unit_label = 'second_unit'
        third_unit_label = 'third_unit'
        span_test_one_label = 'span_test_one'
        span_test_two_label = 'span_test_two'
        expected_first_unit_labels = [first_unit_label, span_test_one_label]
        expected_first_unit_open_price = 100.0
        expected_first_unit_close_price = 95.0
        expected_first_unit_high_price = 110.0
        expected_first_unit_low_price = 900.0
        expected_first_unit_delta = 5
        expected_first_unit_delta_percentage = .04
        expected_second_unit_labels = [second_unit_label, span_test_two_label, span_test_one_label]
        expected_second_unit_open_price = 95.0
        expected_second_unit_close_price = 93.0
        expected_second_unit_high_price = 98.0
        expected_second_unit_low_price = 90.0
        expected_second_unit_delta = -2
        expected_second_unit_delta_percentage = -0.021052632
        self.testSpan.addSpanUnit(expected_first_unit_labels, expected_first_unit_close_price, expected_first_unit_open_price, expected_first_unit_high_price, expected_first_unit_low_price, expected_first_unit_delta, expected_first_unit_delta_percentage)
        self.testSpan.addSpanUnit(expected_second_unit_labels, expected_second_unit_close_price, expected_second_unit_open_price, expected_second_unit_high_price, expected_second_unit_low_price, expected_second_unit_delta, expected_second_unit_delta_percentage)
        # Test the return for the first unit label
        test_first_unit_label_dict = self.testSpan.getSpanUnitsByLabel(first_unit_label)
        self.assertEqual(len(test_first_unit_label_dict), 1)
        test_first_span_unit_index = test_first_unit_label_dict.items()[0][0]
        testFirstSpanUnit = test_first_unit_label_dict.items()[0][1]
        self.assertEqual(test_first_span_unit_index, 0)
        self.assertEqual(expected_first_unit_open_price, testFirstSpanUnit.open_price)
        self.assertEqual(expected_first_unit_close_price, testFirstSpanUnit.close_price)
        self.assertEqual(expected_first_unit_high_price, testFirstSpanUnit.high_price)
        self.assertEqual(expected_first_unit_low_price, testFirstSpanUnit.low_price)
        self.assertEqual(expected_first_unit_delta, testFirstSpanUnit.delta)
        self.assertEqual(expected_first_unit_delta_percentage, testFirstSpanUnit.delta_percentage)
        # Test retrieving a unit which has not been added
        testThirdSpanUnit = self.testSpan.getSpanUnitsByLabel('third_unit')
        self.assertIsNone(testThirdSpanUnit)
        expected_third_unit_open_price = 93.0
        expected_third_unit_close_price = 88.0
        expected_third_unit_high_price = 97.0
        expected_third_unit_low_price = 86.0
        expected_third_unit_delta = -5
        expected_third_unit_delta_percentage = -0.053763441
        expected_close_price_average = 92.0
        expected_third_unit_labels = [third_unit_label, span_test_two_label]
        testThirdSpanUnit = self.testSpan.addSpanUnit(expected_third_unit_labels, expected_third_unit_close_price, expected_third_unit_open_price, expected_third_unit_high_price, expected_third_unit_low_price, expected_third_unit_delta, expected_third_unit_delta_percentage)
        # Test the return for the span test one label
        test_span_one_unit_label_dict = self.testSpan.getSpanUnitsByLabel(span_test_one_label)
        self.assertEqual(len(test_span_one_unit_label_dict), 2)
        test_span_one_first_unit_index = test_span_one_unit_label_dict.items()[0][0]
        self.assertEqual(test_span_one_first_unit_index, 0)
        testSpanOneFirstUnit = test_span_one_unit_label_dict.items()[0][1]
        self.assertEqual(expected_first_unit_open_price, testSpanOneFirstUnit.open_price)
        self.assertEqual(expected_first_unit_close_price, testSpanOneFirstUnit.close_price)
        self.assertEqual(expected_first_unit_high_price, testSpanOneFirstUnit.high_price)
        self.assertEqual(expected_first_unit_low_price, testSpanOneFirstUnit.low_price)
        self.assertEqual(expected_first_unit_delta, testSpanOneFirstUnit.delta)
        self.assertEqual(expected_first_unit_delta_percentage, testSpanOneFirstUnit.delta_percentage)
        test_span_one_second_unit_index = test_span_one_unit_label_dict.items()[1][0]
        self.assertEqual(test_span_one_second_unit_index, 1)
        testSpanOneSecondUnit = test_span_one_unit_label_dict.items()[1][1]
        self.assertEqual(expected_second_unit_open_price, testSpanOneSecondUnit.open_price)
        self.assertEqual(expected_second_unit_close_price, testSpanOneSecondUnit.close_price)
        self.assertEqual(expected_second_unit_high_price, testSpanOneSecondUnit.high_price)
        self.assertEqual(expected_second_unit_low_price, testSpanOneSecondUnit.low_price)
        self.assertEqual(expected_second_unit_delta, testSpanOneSecondUnit.delta)
        self.assertEqual(expected_second_unit_delta_percentage, testSpanOneSecondUnit.delta_percentage)
        # Test the return for the span test two label
        test_span_two_unit_label_dict = self.testSpan.getSpanUnitsByLabel(span_test_two_label)
        self.assertEqual(len(test_span_two_unit_label_dict), 2)
        test_span_two_first_unit_index = test_span_two_unit_label_dict.items()[0][0]
        self.assertEqual(test_span_two_first_unit_index, 1)
        testSpanTwoFirstUnit = test_span_two_unit_label_dict.items()[0][1]
        self.assertEqual(expected_second_unit_open_price, testSpanTwoFirstUnit.open_price)
        self.assertEqual(expected_second_unit_close_price, testSpanTwoFirstUnit.close_price)
        self.assertEqual(expected_second_unit_high_price, testSpanTwoFirstUnit.high_price)
        self.assertEqual(expected_second_unit_low_price, testSpanTwoFirstUnit.low_price)
        self.assertEqual(expected_second_unit_delta, testSpanTwoFirstUnit.delta)
        self.assertEqual(expected_second_unit_delta_percentage, testSpanTwoFirstUnit.delta_percentage)
        test_span_two_second_unit_index = test_span_two_unit_label_dict.items()[1][0]
        self.assertEqual(test_span_two_second_unit_index, 2)
        testSpanTwoSecondUnit = test_span_two_unit_label_dict.items()[1][1]
        self.assertEqual(expected_third_unit_open_price, testSpanTwoSecondUnit.open_price)
        self.assertEqual(expected_third_unit_close_price, testSpanTwoSecondUnit.close_price)
        self.assertEqual(expected_third_unit_high_price, testSpanTwoSecondUnit.high_price)
        self.assertEqual(expected_third_unit_low_price, testSpanTwoSecondUnit.low_price)
        self.assertEqual(expected_third_unit_delta, testSpanTwoSecondUnit.delta)
        self.assertEqual(expected_third_unit_delta_percentage, testSpanTwoSecondUnit.delta_percentage)
        test_close_price_average = self.testSpan.getSpanCloseAverage()
        self.assertEqual(expected_close_price_average, test_close_price_average)
        # Test Span Deltas
        expected_span_delta = -12.0
        test_span_delta = self.testSpan.getSpanDelta()
        self.assertEqual(expected_span_delta, test_span_delta)
        expected_span_delta_percentage = expected_span_delta / expected_first_unit_open_price
        test_span_delta_percentage = self.testSpan.getSpanDelta(get_percentage_delta = True)
        self.assertEqual(expected_span_delta_percentage, test_span_delta_percentage)
        # Test Span Deltas with unit label
        test_span_test_two_label_delta = self.testSpan.getSpanDelta(span_test_two_label)
        expected_span_test_two_label_delta = -7.0
        self.assertEqual(expected_span_test_two_label_delta, test_span_test_two_label_delta)
        test_span_test_two_label_delta_percentage = round(self.testSpan.getSpanDelta(span_test_two_label, get_percentage_delta = True), 6)
        expected_span_test_two_label_delta_percentage = round(expected_span_test_two_label_delta / expected_second_unit_open_price, 6)
        self.assertEqual(expected_span_test_two_label_delta_percentage, test_span_test_two_label_delta_percentage)
        test_span_test_one_label_delta = self.testSpan.getSpanDelta(span_test_one_label)
        expected_span_test_one_label_delta = -7.0
        self.assertEqual(expected_span_test_one_label_delta, test_span_test_one_label_delta)
        test_span_test_one_label_delta_percentage = round(self.testSpan.getSpanDelta(span_test_one_label, get_percentage_delta = True), 6)
        expected_span_test_one_label_delta_percentage = round(expected_span_test_one_label_delta / expected_first_unit_open_price, 6)
        self.assertEqual(expected_span_test_one_label_delta_percentage, test_span_test_one_label_delta_percentage)

if __name__ == '__main__':
    unittest.main()
