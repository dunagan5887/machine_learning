import unittest
from dateInterval import DateInterval
from dateInterval import DateIntervalDictionary


class TestDateInterval(unittest.TestCase):

    def tearDown(self):
        self.testDateIntervalInstance = None
        self.interval_start = None
        self.interval_end = None
        self.interval_code = None

    def setUp(self):
        self.interval_start = '2015-12-29'
        self.interval_end = '2016-01-02'
        self.interval_code = 'unit_test'
        self.testDateIntervalInstance = DateInterval(self.interval_start, self.interval_end, self.interval_code)  # type: DateInterval

    def test_init(self):
        test_start_date = self.testDateIntervalInstance.start_date
        self.assertEqual(test_start_date, self.interval_start)
        test_end_date = self.testDateIntervalInstance.end_date
        self.assertEqual(test_end_date, self.interval_end)
        test_code = self.testDateIntervalInstance.code
        self.assertEqual(test_code, self.interval_code)

    def test_isDateInInterval(self):
        dates_that_are_in_interval = ['2015-12-30', '2015-12-29', '2015-12-31', '2016-01-01', '2016-01-02']
        for date_string in dates_that_are_in_interval:
            result = self.testDateIntervalInstance.isDateInInterval(date_string)
            self.assertTrue(result)
        dates_not_in_interval = ['2015-12-28', '2015-11-30', '2014-12-31', '2016-01-03', '2015-01-01']
        for date_string in dates_not_in_interval:
            result = self.testDateIntervalInstance.isDateInInterval(date_string)
            self.assertFalse(result)


class TestDateIntervalDictionary(unittest.TestCase):

    def tearDown(self):
        self.dateIntervalOne = None
        self.dateIntervalTwo = None
        self.dateIntervalThree = None

        self.dateIntervalDictionaryInstance = None

    def setUp(self):
        self.interval_start_one = '2015-12-29'
        self.interval_end_one = '2016-01-02'
        self.interval_code_one = 'unit_test_one'
        self.dateIntervalOne = DateInterval(self.interval_start_one, self.interval_end_one, self.interval_code_one)
        self.interval_start_two = '2016-01-02'
        self.interval_end_two = '2016-01-12'
        self.interval_code_two = 'unit_test_two'
        self.dateIntervalTwo = DateInterval(self.interval_start_two, self.interval_end_two, self.interval_code_two)
        self.interval_start_three = '2015-12-12'
        self.interval_end_three = '2015-12-20'
        self.interval_code_three = 'unit_test_three'
        self.dateIntervalThree = DateInterval(self.interval_start_three, self.interval_end_three, self.interval_code_three)
        self.test_dictionary_code = 'unit_test_dictionary'
        self.dateIntervalDictionaryInstance = DateIntervalDictionary(self.test_dictionary_code)  # type: DateIntervalDictionary
        self.dateIntervalDictionaryInstance.addDateInterval(self.dateIntervalOne)
        self.dateIntervalDictionaryInstance.addDateInterval(self.dateIntervalTwo)
        self.dateIntervalDictionaryInstance.addDateInterval(self.dateIntervalThree)

    def test_init(self):
        test_code = self.dateIntervalDictionaryInstance.dictionary_code
        self.assertEqual(test_code, self.test_dictionary_code)

    def test_addDateInterval(self):
        dictionary_length = len(self.dateIntervalDictionaryInstance.date_interval_dictionary)
        self.assertEqual(dictionary_length, 3)

    def getDateIntervalByCode(self):
        testDateIntervalTwo = self.dateIntervalDictionaryInstance.getDateIntervalByCode(self.interval_code_two)
        self.assertIsNotNone(testDateIntervalTwo)
        self.assertEqual(self.interval_start_two, testDateIntervalTwo.start_date)
        self.assertEqual(self.interval_end_two, testDateIntervalTwo.end_date)
        self.assertEqual(self.interval_code_two, testDateIntervalTwo.code)
        noneInterval = self.dateIntervalDictionaryInstance.getDateIntervalByCode('fake_interval')
        self.assertIsNone(noneInterval)

    def test_getIntervalCodesByDate(self):
        test_dates_for_interval_one = ['2015-12-29', '2016-01-01', '2015-12-31']
        interval_one_code_list = [self.interval_code_one]
        test_dates_for_interval_two = ['2016-01-12', '2016-01-07']
        interval_two_code_list = [self.interval_code_two]
        test_dates_for_interval_three = ['2015-12-12', '2015-12-20', '2015-12-16']
        interval_three_code_list = [self.interval_code_three]
        date_in_intervals_one_and_two = '2016-01-02'
        expected_interval_one_and_two_list = [self.interval_code_one, self.interval_code_two]
        for date_string in test_dates_for_interval_one:
            test_list_of_codes = self.dateIntervalDictionaryInstance.getIntervalCodesByDate(date_string)
            self.assertEqual(test_list_of_codes, interval_one_code_list, 'Failed for date {0}'.format(date_string))
        for date_string in test_dates_for_interval_two:
            test_list_of_codes = self.dateIntervalDictionaryInstance.getIntervalCodesByDate(date_string)
            self.assertEqual(test_list_of_codes, interval_two_code_list, 'Failed for date {0}'.format(date_string))
        for date_string in test_dates_for_interval_three:
            test_list_of_codes = self.dateIntervalDictionaryInstance.getIntervalCodesByDate(date_string)
            self.assertEqual(test_list_of_codes, interval_three_code_list, 'Failed for date {0}'.format(date_string))
        test_interval_one_and_two_list = self.dateIntervalDictionaryInstance.getIntervalCodesByDate(date_in_intervals_one_and_two)
        self.assertEqual(test_interval_one_and_two_list, expected_interval_one_and_two_list)

if __name__ == '__main__':
    unittest.main()
