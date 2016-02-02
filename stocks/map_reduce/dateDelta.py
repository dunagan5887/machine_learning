from datetime import timedelta
from datetime import datetime


class DateDelta:

    seconds_in_a_day = 60 * 60 * 24

    @staticmethod
    def getDaysBetweenTodayAndDateString(date_string):
        today_datetime = datetime.today()
        date_from = datetime.strptime(date_string, '%Y-%m-%d')
        return DateDelta.getDaysBetweenDates(date_from, today_datetime)

    @staticmethod
    def getDaysBetweenDateStrings(before_date_string, after_date_string):
        before_datetime = datetime.strptime(before_date_string, '%Y-%m-%d')
        after_datetime = datetime.strptime(after_date_string, '%Y-%m-%d')
        return DateDelta.getDaysBetweenDates(before_datetime, after_datetime)

    @staticmethod
    def getDaysBetweenDates(before_datetime, after_datetime):
        total_seconds_between_dates = DateDelta.getTotalSecondsBetweenDates(before_datetime, after_datetime)
        days_since = int(total_seconds_between_dates) / int(DateDelta.seconds_in_a_day)
        return days_since

    @staticmethod
    def getTotalSecondsBetweenDates(before_datetime, after_datetime):
        diff_in_dates = after_datetime - before_datetime
        total_seconds = diff_in_dates.total_seconds()
        return total_seconds

