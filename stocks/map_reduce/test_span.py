from span import Span
from span import SpanUnit

import unittest

class TestSpan(unittest.TestCase):

    def test_spanInit(self):
        span_code = 'test_span'
        testSpan = Span(span_code)
        self.assertEquals(span_code, testSpan.code, 'The code returned from a Span object does not match the code used to initialize the object')

    def test_addSpanUnit(self):
        span_code = 'test_span'
        testSpan = Span(span_code)
        span_delta = testSpan.getSpanDelta()
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
        testSpan.addSpanUnit(expected_first_unit_labels, expected_first_unit_close_price, expected_first_unit_open_price, expected_first_unit_high_price, expected_first_unit_low_price, expected_first_unit_delta, expected_first_unit_delta_percentage)
        testSpan.addSpanUnit(expected_second_unit_labels, expected_second_unit_close_price, expected_second_unit_open_price, expected_second_unit_high_price, expected_second_unit_low_price, expected_second_unit_delta, expected_second_unit_delta_percentage)
        # Test the return for the first unit label
        test_first_unit_label_dict = testSpan.getSpanUnitsByLabel(first_unit_label)
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
        testThirdSpanUnit = testSpan.getSpanUnitsByLabel('third_unit')
        self.assertIsNone(testThirdSpanUnit)
        expected_third_unit_open_price = 93.0
        expected_third_unit_close_price = 88.0
        expected_third_unit_high_price = 97.0
        expected_third_unit_low_price = 86.0
        expected_third_unit_delta = -5
        expected_third_unit_delta_percentage = -0.053763441
        expected_close_price_average = 92.0
        expected_third_unit_labels = [third_unit_label, span_test_two_label]
        testThirdSpanUnit = testSpan.addSpanUnit(expected_third_unit_labels, expected_third_unit_close_price, expected_third_unit_open_price, expected_third_unit_high_price, expected_third_unit_low_price, expected_third_unit_delta, expected_third_unit_delta_percentage)
        # Test the return for the span test one label
        test_span_one_unit_label_dict = testSpan.getSpanUnitsByLabel(span_test_one_label)
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
        test_span_two_unit_label_dict = testSpan.getSpanUnitsByLabel(span_test_two_label)
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
        test_close_price_average = testSpan.getSpanCloseAverage()
        self.assertEqual(expected_close_price_average, test_close_price_average)
        # Test Span Deltas
        expected_span_delta = -12.0
        test_span_delta = testSpan.getSpanDelta()
        self.assertEqual(expected_span_delta, test_span_delta)
        expected_span_delta_percentage = expected_span_delta / expected_first_unit_open_price
        test_span_delta_percentage = testSpan.getSpanDelta(get_percentage_delta = True)
        self.assertEqual(expected_span_delta_percentage, test_span_delta_percentage)
        # Test Span Deltas with unit label
        test_span_test_two_label_delta = testSpan.getSpanDelta(span_test_two_label)
        expected_span_test_two_label_delta = -7.0
        self.assertEqual(expected_span_test_two_label_delta, test_span_test_two_label_delta)
        test_span_test_two_label_delta_percentage = round(testSpan.getSpanDelta(span_test_two_label, get_percentage_delta = True), 6)
        expected_span_test_two_label_delta_percentage = round(expected_span_test_two_label_delta / expected_second_unit_open_price, 6)
        self.assertEqual(expected_span_test_two_label_delta_percentage, test_span_test_two_label_delta_percentage)
        test_span_test_one_label_delta = testSpan.getSpanDelta(span_test_one_label)
        expected_span_test_one_label_delta = -7.0
        self.assertEqual(expected_span_test_one_label_delta, test_span_test_one_label_delta)
        test_span_test_one_label_delta_percentage = round(testSpan.getSpanDelta(span_test_one_label, get_percentage_delta = True), 6)
        expected_span_test_one_label_delta_percentage = round(expected_span_test_one_label_delta / expected_first_unit_open_price, 6)
        self.assertEqual(expected_span_test_one_label_delta_percentage, test_span_test_one_label_delta_percentage)

if __name__ == '__main__':
    unittest.main()
