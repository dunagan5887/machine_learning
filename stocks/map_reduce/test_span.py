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
        expected_first_unit_code = 'first_unit'
        expected_first_unit_open_price = 100.0
        expected_first_unit_close_price = 95.0
        expected_first_unit_high_price = 110.0
        expected_first_unit_low_price = 900.0
        expected_first_unit_delta = 5
        expected_first_unit_delta_percentage = .04
        expected_second_unit_code = 'second_unit'
        expected_second_unit_open_price = 95.0
        expected_second_unit_close_price = 93.0
        expected_second_unit_high_price = 98.0
        expected_second_unit_low_price = 90.0
        expected_second_unit_delta = -2
        expected_second_unit_delta_percentage = -0.021052632
        testSpan.addSpanUnit(expected_first_unit_code, expected_first_unit_close_price, expected_first_unit_open_price, expected_first_unit_high_price, expected_first_unit_low_price, expected_first_unit_delta, expected_first_unit_delta_percentage)
        testSpan.addSpanUnit(expected_second_unit_code, expected_second_unit_close_price, expected_second_unit_open_price, expected_second_unit_high_price, expected_second_unit_low_price, expected_second_unit_delta, expected_second_unit_delta_percentage)
        testFirstSpanUnit = testSpan.getSpanUnitByCode(expected_first_unit_code)
        self.assertEqual(expected_first_unit_code, testFirstSpanUnit.code)
        self.assertEqual(expected_first_unit_open_price, testFirstSpanUnit.open_price)
        self.assertEqual(expected_first_unit_close_price, testFirstSpanUnit.close_price)
        self.assertEqual(expected_first_unit_high_price, testFirstSpanUnit.high_price)
        self.assertEqual(expected_first_unit_low_price, testFirstSpanUnit.low_price)
        self.assertEqual(expected_first_unit_delta, testFirstSpanUnit.delta)
        self.assertEqual(expected_first_unit_delta_percentage, testFirstSpanUnit.delta_percentage)
        testSecondSpanUnit = testSpan.getSpanUnitByCode(expected_second_unit_code)
        self.assertEqual(expected_second_unit_code, testSecondSpanUnit.code)
        self.assertEqual(expected_second_unit_open_price, testSecondSpanUnit.open_price)
        self.assertEqual(expected_second_unit_close_price, testSecondSpanUnit.close_price)
        self.assertEqual(expected_second_unit_high_price, testSecondSpanUnit.high_price)
        self.assertEqual(expected_second_unit_low_price, testSecondSpanUnit.low_price)
        self.assertEqual(expected_second_unit_delta, testSecondSpanUnit.delta)
        self.assertEqual(expected_second_unit_delta_percentage, testSecondSpanUnit.delta_percentage)
        testThirdSpanUnit = testSpan.getSpanUnitByCode('third_unit')
        self.assertIsNone(testThirdSpanUnit)
        expected_third_unit_code = 'third_unit'
        expected_third_unit_open_price = 93.0
        expected_third_unit_close_price = 88.0
        expected_third_unit_high_price = 97.0
        expected_third_unit_low_price = 86.0
        expected_third_unit_delta = -5
        expected_third_unit_delta_percentage = -0.053763441
        expected_close_price_average = 92.0
        testThirdSpanUnit = testSpan.addSpanUnit(expected_third_unit_code, expected_third_unit_close_price, expected_third_unit_open_price, expected_third_unit_high_price, expected_third_unit_low_price, expected_third_unit_delta, expected_third_unit_delta_percentage)
        test_close_price_average = testSpan.getSpanCloseAverage()
        self.assertEqual(expected_close_price_average, test_close_price_average)
        expected_span_delta = -12.0
        test_span_delta = testSpan.getSpanDelta()
        self.assertEqual(expected_span_delta, test_span_delta)

if __name__ == '__main__':
    unittest.main()
