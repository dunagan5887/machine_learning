import unittest
from datetime import datetime
from datetime import timedelta
from dateDelta import DateDelta


class TestDateDelta(unittest.TestCase):

    def tearDown(self):
        self.before_date_string = None
        self.before_datetime = None
        self.after_date_string = None
        self.after_datetime = None
        self.seconds_between_dates = None
        self.days_between_dates = None

    def setUp(self):
        self.before_date_string = '2015-12-29'
        self.before_datetime = datetime.strptime(self.before_date_string, '%Y-%m-%d')
        self.after_date_string = '2016-01-19'
        self.after_datetime = datetime.strptime(self.after_date_string, '%Y-%m-%d')
        self.seconds_between_dates = 1814400.0
        self.days_between_dates = 21

    def test_getTotalSecondsBetweenDates(self):
        total_seconds_between_dates = DateDelta.getTotalSecondsBetweenDates(self.before_datetime, self.after_datetime)
        self.assertEqual(total_seconds_between_dates, self.seconds_between_dates)

    def test_getDaysBetweenDates(self):
        days_between_dates = DateDelta.getDaysBetweenDates(self.before_datetime, self.after_datetime)
        self.assertEqual(days_between_dates, self.days_between_dates)

    def test_getDaysBetweenDateStrings(self):
        days_between_date_strings = DateDelta.getDaysBetweenDateStrings(self.before_date_string, self.after_date_string)
        self.assertEqual(self.days_between_dates, days_between_date_strings)

    def test_getDaysBetweenTodayAndDateString(self):
        test_days_between_today_and_date = DateDelta.getDaysBetweenTodayAndDateString(self.before_date_string)
        today_datetime = datetime.today()
        diff_in_dates = today_datetime - self.before_datetime
        total_seconds = diff_in_dates.total_seconds()
        expected_days_since = int(total_seconds) / int(DateDelta.seconds_in_a_day)
        self.assertEqual(test_days_between_today_and_date, expected_days_since)

if __name__ == '__main__':
    unittest.main()
