import unittest
from dateInterval import DateInterval
from dateInterval import DateIntervalDictionary
from dateInterval import DateIntervalFactory
from collections import OrderedDict


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
        self.interval_code_four = 'unit_test_four'
        self.start_date_four = '2016-01-31'
        self.end_date_four = '2016-02-15'
        self.dateIntervalDictionaryInstance.addDateIntervalByDates(self.start_date_four, self.end_date_four, self.interval_code_four)
        self.expected_date_interval_codes = [self.interval_code_one, self.interval_code_two, self.interval_code_three, self.interval_code_four]
        self.expected_date_interval_codes.sort()
        self.expected_earliest_date = self.interval_start_three
        self.expected_latest_date = self.end_date_four

    def test_getEarliestDateInDictionary(self):
        test_earliest_date = self.dateIntervalDictionaryInstance.getEarliestDateInDictionary()
        self.assertEqual(test_earliest_date, self.expected_earliest_date)

    def test_getLatestDateInDictionary(self):
        test_latest_date = self.dateIntervalDictionaryInstance.getLatestDateInDictionary()
        self.assertEqual(test_latest_date, self.expected_latest_date)

    def test_getDateIntervalCodes(self):
        test_date_intervals = self.dateIntervalDictionaryInstance.getDateIntervalCodes()
        test_date_intervals.sort()
        self.assertListEqual(self.expected_date_interval_codes, test_date_intervals)

    def test_init(self):
        test_code = self.dateIntervalDictionaryInstance.dictionary_code
        self.assertEqual(test_code, self.test_dictionary_code)

    def test_addDateInterval(self):
        dictionary_length = len(self.dateIntervalDictionaryInstance.date_interval_dictionary)
        self.assertEqual(dictionary_length, 4)

    def test_addDateIntervalByDates(self):
        testDateIntervalFour = self.dateIntervalDictionaryInstance.getDateIntervalByCode(self.interval_code_four)
        self.assertEqual(testDateIntervalFour.code, self.interval_code_four)

    def test_getDateIntervalByCode(self):
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
        test_interval_four_dates = ['2016-01-31', '2016-02-02', '2016-02-15']
        interval_four_code_list = [self.interval_code_four]
        for date_string in test_interval_four_dates:
            test_list_of_codes = self.dateIntervalDictionaryInstance.getIntervalCodesByDate(date_string)
            self.assertEqual(test_list_of_codes, interval_four_code_list, 'Failed for date {0}'.format(date_string))
        test_dates_in_no_intervals = ['2015-12-11', '2015-12-21', '2015-12-28', '2016-01-30', '2016-02-16']
        for date_string in test_dates_in_no_intervals:
            test_none = self.dateIntervalDictionaryInstance.getIntervalCodesByDate(date_string)
            self.assertIsNone(test_none)

class TestDateIntervalFactory(unittest.TestCase):

    def tearDown(self):
        self.dictionary_of_date_intervals = None

    def setUp(self):
        self.start_date = '2015-12-29'
        self.frequency_one = 3
        self.count_one = 4
        self.unit_one = 'weeks'
        self.direction_one = True
        self.test_dictionary_of_date_intervals_one = DateIntervalFactory.getDateIntervalDates(self.start_date, self.frequency_one, self.count_one, self.unit_one, self.direction_one)
        self.expected_interval_dates_one = OrderedDict()
        self.expected_interval_dates_one['0-3_weeks'] = ['2015-12-08', '2015-12-29']
        self.expected_interval_dates_one['3-6_weeks'] = ['2015-11-17', '2015-12-08']
        self.expected_interval_dates_one['6-9_weeks'] = ['2015-10-27', '2015-11-17']
        self.expected_interval_dates_one['9-12_weeks'] = ['2015-10-06', '2015-10-27']

        self.frequency_two = 30
        self.count_two = 3
        self.unit_two = 'days'
        self.direction_two = False
        self.test_dictionary_of_date_intervals_two = DateIntervalFactory.getDateIntervalDates(self.start_date, self.frequency_two, self.count_two, self.unit_two, self.direction_two)
        self.expected_interval_dates_two = OrderedDict()
        self.expected_interval_dates_two['0-30_days'] = ['2015-12-29', '2016-01-28']
        self.expected_interval_dates_two['30-60_days'] = ['2016-01-28', '2016-02-27']
        self.expected_interval_dates_two['60-90_days'] = ['2016-02-27', '2016-03-28']

        self.start_date_three = '2015-12-29'
        self.frequency_three = 21
        self.count_three = 4
        self.unit_three = 'days'
        self.direction_three = False
        self.code_three = 'test_date_interval_dictionary'
        self.test_date_interval_dictionary = DateIntervalFactory.getDateIntervalDictionary(self.start_date, self.frequency_three, self.count_three, self.unit_three, self.direction_three, self.code_three)
        self.test_dates_interval_three_code_one = '0-21_days'
        self.test_dates_interval_three_code_two = '21-42_days'
        self.test_dates_interval_three_code_three = '42-63_days'
        self.test_dates_interval_three_code_four = '63-84_days'
        self.expected_interval_dates_three = OrderedDict()
        self.expected_interval_dates_three[self.test_dates_interval_three_code_one] = ['2015-12-29', '2016-01-19']
        self.expected_interval_dates_three[self.test_dates_interval_three_code_two] = ['2016-01-19', '2016-02-09']
        self.expected_interval_dates_three[self.test_dates_interval_three_code_three] = ['2016-02-09', '2016-03-01']
        self.expected_interval_dates_three[self.test_dates_interval_three_code_four] = ['2016-03-01', '2016-03-22']
        self.test_interval_one_dates_list = ['2015-12-29', '2015-12-31', '2016-01-10']
        self.test_interval_one_and_two_dates_list = ['2016-01-19']
        self.test_interval_two_dates_list = ['2016-01-29', '2016-02-05']
        self.test_interval_two_and_three_dates_list = ['2016-02-09']
        self.test_interval_three_dates_list = ['2016-02-19']
        self.test_interval_three_and_four_dates_list = ['2016-03-01']
        self.test_interval_four_dates_list = ['2016-03-15', '2016-03-22']
        self.test_no_interval_list = ['2014-12-30', '2015-11-30', '2015-12-28', '2016-03-23', '2016-04-23', '2017-03-20']

        self.start_date_four = '2015-12-29'
        self.frequency_four = 21
        self.count_four = 4
        self.unit_four = 'days'
        self.direction_four = True
        self.code_four = 'test_date_interval_dictionary_backwards'
        self.test_date_interval_dictionary_backwards = DateIntervalFactory.getDateIntervalDictionary(self.start_date, self.frequency_four, self.count_four, self.unit_four, self.direction_four, self.code_four)
        self.test_dates_interval_four_code_one = '0-21_days'
        self.test_dates_interval_four_code_two = '21-42_days'
        self.test_dates_interval_four_code_three = '42-63_days'
        self.test_dates_interval_four_code_four = '63-84_days'

    def test_getIntervalCodesByDate(self):
        test_date_one = '2015-12-19'
        test_date_two = '2016-01-24'
        test_date_three = '2016-02-09'
        test_interval_codes_for_date_one_forwards = self.test_date_interval_dictionary.getIntervalCodesByDate(test_date_one)
        self.assertIsNone(test_interval_codes_for_date_one_forwards)
        test_interval_codes_for_date_two_forwards = self.test_date_interval_dictionary.getIntervalCodesByDate(test_date_two)
        expected_interval_codes_for_date_two_forwards = [self.test_dates_interval_three_code_two]
        self.assertEqual(test_interval_codes_for_date_two_forwards, expected_interval_codes_for_date_two_forwards)
        test_interval_codes_for_date_three_forwards = self.test_date_interval_dictionary.getIntervalCodesByDate(test_date_three)
        expected_interval_codes_for_date_three_forwards = [self.test_dates_interval_three_code_two, self.test_dates_interval_three_code_three]
        self.assertEqual(test_interval_codes_for_date_three_forwards, expected_interval_codes_for_date_three_forwards)

        test_interval_codes_for_date_one_backwards = self.test_date_interval_dictionary_backwards.getIntervalCodesByDate(test_date_one)
        expected_interval_codes_for_date_one_backwards = [self.test_dates_interval_four_code_one]
        self.assertEqual(test_interval_codes_for_date_one_backwards, expected_interval_codes_for_date_one_backwards)
        test_interval_codes_for_date_two_backwards = self.test_date_interval_dictionary_backwards.getIntervalCodesByDate(test_date_two)
        self.assertIsNone(test_interval_codes_for_date_two_backwards)
        test_date_four = '2015-12-08'
        test_interval_codes_for_date_four_backwards = self.test_date_interval_dictionary_backwards.getIntervalCodesByDate(test_date_four)
        expected_interval_codes_for_date_four_backwards = [self.test_dates_interval_four_code_one, self.test_dates_interval_four_code_two]
        self.assertEqual(test_interval_codes_for_date_four_backwards, expected_interval_codes_for_date_four_backwards)
        test_date_five = '2015-10-15'
        test_interval_codes_for_date_five_backwards = self.test_date_interval_dictionary_backwards.getIntervalCodesByDate(test_date_five)
        expected_interval_codes_for_date_five_backwards = [self.test_dates_interval_four_code_four]
        self.assertEqual(test_interval_codes_for_date_five_backwards, expected_interval_codes_for_date_five_backwards)

    def test_getDateIntervalDates(self):
        self.assertEqual(self.expected_interval_dates_one, self.test_dictionary_of_date_intervals_one)
        self.assertEqual(self.expected_interval_dates_two, self.test_dictionary_of_date_intervals_two)

    def test_getDateIntervalDictionary(self):
        self.assertEqual(self.code_three, self.test_date_interval_dictionary.dictionary_code)
        for date_string in self.test_interval_one_dates_list:
            test_list_of_codes = self.test_date_interval_dictionary.getIntervalCodesByDate(date_string)
            self.assertEqual(test_list_of_codes, [self.test_dates_interval_three_code_one], 'Failed for date {0}'.format(date_string))
        for date_string in self.test_interval_two_dates_list:
            test_list_of_codes = self.test_date_interval_dictionary.getIntervalCodesByDate(date_string)
            self.assertEqual(test_list_of_codes, [self.test_dates_interval_three_code_two], 'Failed for date {0}'.format(date_string))
        for date_string in self.test_interval_three_dates_list:
            test_list_of_codes = self.test_date_interval_dictionary.getIntervalCodesByDate(date_string)
            self.assertEqual(test_list_of_codes, [self.test_dates_interval_three_code_three], 'Failed for date {0}'.format(date_string))
        for date_string in self.test_interval_four_dates_list:
            test_list_of_codes = self.test_date_interval_dictionary.getIntervalCodesByDate(date_string)
            self.assertEqual(test_list_of_codes, [self.test_dates_interval_three_code_four], 'Failed for date {0}'.format(date_string))
        for date_string in self.test_interval_one_and_two_dates_list:
            test_list_of_codes = self.test_date_interval_dictionary.getIntervalCodesByDate(date_string)
            self.assertEqual(test_list_of_codes, [self.test_dates_interval_three_code_one, self.test_dates_interval_three_code_two], 'Failed for date {0}'.format(date_string))
        for date_string in self.test_interval_two_and_three_dates_list:
            test_list_of_codes = self.test_date_interval_dictionary.getIntervalCodesByDate(date_string)
            self.assertEqual(test_list_of_codes, [self.test_dates_interval_three_code_two, self.test_dates_interval_three_code_three], 'Failed for date {0}'.format(date_string))
        for date_string in self.test_interval_three_and_four_dates_list:
            test_list_of_codes = self.test_date_interval_dictionary.getIntervalCodesByDate(date_string)
            self.assertEqual(test_list_of_codes, [self.test_dates_interval_three_code_three, self.test_dates_interval_three_code_four], 'Failed for date {0}'.format(date_string))


if __name__ == '__main__':
    unittest.main()
