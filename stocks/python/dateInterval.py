from datetime import timedelta
from datetime import datetime
from collections import OrderedDict
from dateDelta import DateDelta
import time

class DateInterval:
    
    def __init__(self, start_date, end_date, code = None):
        self.start_date = start_date
        self.end_date = end_date
        self.code = code
        self.days_in_interval = DateDelta.getDaysBetweenDateStrings(self.start_date, self.end_date)

    def isDateInInterval(self, date_to_compare):
        """
        :param string date_to_compare:
        :return: bool
        """
        return ((date_to_compare >= self.start_date) and (date_to_compare <= self.end_date))

    def getNumberOfDaysInInterval(self):
        """
        :return: int
        """
        return self.days_in_interval

    @staticmethod
    def getYesterdayDate():
        today_date = time.strftime("%Y-%m-%d")
        datetime_to_act_from = datetime.strptime(today_date, '%Y-%m-%d')
        method_arguments_dict = {'days': 1}
        one_day_delta = timedelta(**method_arguments_dict)
        interval_datetime = datetime_to_act_from - one_day_delta
        interval_date_string = interval_datetime.strftime('%Y-%m-%d')
        return interval_date_string

class DateIntervalDictionary:
    
    def __init__(self, dictionary_code):
        """
        :param string dictionary_code:
        :return:
        """
        self.dictionary_code = dictionary_code
        self.date_interval_dictionary = OrderedDict()
        self.earliest_date = None
        self.latest_date = None
        self.number_of_days_in_dictionary = 0

    def getDatabaseTableName(self, table_suffix):
        table_name = self.dictionary_code + '_' + table_suffix
        underscored_table_name = table_name.replace('-', '_')
        return underscored_table_name

    def getEarliestDateInDictionary(self):
        return self.earliest_date

    def getLatestDateInDictionary(self):
        return self.latest_date

    def getDateIntervalCodes(self):
        """
        :return: list
        """
        return self.date_interval_dictionary.keys()

    def addDateIntervalByDates(self, start_date, end_date, interval_code = None):
        dateIntervalObject = DateInterval(start_date, end_date, interval_code)
        return self.addDateInterval(dateIntervalObject, interval_code)

    def addDateInterval(self, dateIntervalInstance, interval_code = None):
        """
        :param DateInterval dateIntervalInstance:
        :param string interval_code:
        :return: DateIntervalDictionary self
        """
        if (interval_code is None):
            interval_code = dateIntervalInstance.code
            if interval_code is None:
                interval_code = len(self.date_interval_dictionary)
        self.date_interval_dictionary[interval_code] = dateIntervalInstance

        start_date = dateIntervalInstance.start_date
        end_date = dateIntervalInstance.end_date

        if (self.earliest_date is None) or (start_date < self.earliest_date):
            self.earliest_date = start_date
        if (self.latest_date is None) or (end_date > self.latest_date):
            self.latest_date = end_date

        days_in_interval = dateIntervalInstance.getNumberOfDaysInInterval()
        self.number_of_days_in_dictionary = self.number_of_days_in_dictionary + days_in_interval

        return self

    def getNumberOfDaysInDictionary(self):
        """
        :return: int
        """
        return self.number_of_days_in_dictionary

    def getDateIntervalByCode(self, interval_code):
        """
        :param string interval_code:
        :return: DateInterval|None
        """
        if interval_code in self.date_interval_dictionary:
            return self.date_interval_dictionary[interval_code]
        return None

    def getIntervalCodesByDate(self, date_to_retrieve_code_for):
        """
        :param string date_to_retrieve_code_for:
        :return: list|None
        """
        list_of_codes = []
        for code, dateIntervalInstance in self.date_interval_dictionary.items():  # type: DateInterval
            if dateIntervalInstance.isDateInInterval(date_to_retrieve_code_for):
                list_of_codes.append(code)
        return list_of_codes if (len(list_of_codes) > 0) else None


class DateIntervalFactory:

    @staticmethod
    def getDateIntervalDictionary(start_date, frequency, count, unit = None, direction_is_past = None, dictionary_code = None):
        """
        :param start_date:
        :param frequency:
        :param count:
        :param unit:
        :param direction_is_past:
        :param dictionary_code:
        :return:DateIntervalDictionary
        """
        dateIntervalDictionaryInstance = DateIntervalDictionary(dictionary_code)  # type: DateIntervalDictionary
        date_intervals_list = DateIntervalFactory.getDateIntervals(start_date, frequency, count, unit, direction_is_past)
        for dateInterval in date_intervals_list:
            dateIntervalDictionaryInstance.addDateInterval(dateInterval, dateInterval.code)
        return dateIntervalDictionaryInstance

    @staticmethod
    def getDateIntervals(start_date, frequency, count, unit = None, direction_is_past = None):
        """
        :param start_date:
        :param frequency:
        :param count:
        :param unit:
        :param direction_is_past:
        :return: list of DateInterval
        """
        date_interval_dates_dict = DateIntervalFactory.getDateIntervalDates(start_date, frequency, count, unit, direction_is_past)
        date_intervals_list = []
        for code, date_interval_dates_dict in date_interval_dates_dict.items():
            start_date = date_interval_dates_dict[0]
            end_date = date_interval_dates_dict[1]
            dateInterval = DateInterval(start_date, end_date, code)
            date_intervals_list.append(dateInterval)
        return date_intervals_list

    @staticmethod
    def getDateIntervalDates(start_date, frequency, count, unit = None, direction_is_past = None):
        """
        :param string start_date: %Y-%m-%d format required
        :param int frequency:
        :param int count:
        :param string unit:
        :param bool direction_is_past:
        :return: OrderedDict
        """
        if (unit is None):
            unit = "weeks"
        if (direction_is_past is None):
            direction_is_past = True
        date_interval_dates_dict = OrderedDict()
        datetime_to_act_from = datetime.strptime(start_date, '%Y-%m-%d')
        datetime_to_act_from_string = start_date
        for i in range(0, count):
            method_arguments_dict = {unit: frequency}
            time_delta = timedelta(**method_arguments_dict)
            interval_datetime = datetime_to_act_from - time_delta if direction_is_past else datetime_to_act_from + time_delta
            interval_date_string = interval_datetime.strftime('%Y-%m-%d')
            code_label = str(i * frequency) + '_' + str((i+1) * frequency)
            code = code_label + '_' + str(unit)
            if direction_is_past:
                date_interval_dates_dict[code] = [interval_date_string, datetime_to_act_from_string]
            else:
                date_interval_dates_dict[code] = [datetime_to_act_from_string, interval_date_string]
            datetime_to_act_from = interval_datetime
            datetime_to_act_from_string = interval_date_string

        # We want the date_interval_dates_dict to be ordered chronologically
        if direction_is_past:
            reordered_interval_dates_dict = OrderedDict()
            interval_dates_items = date_interval_dates_dict.items()
            while interval_dates_items:
                date_interval_tuple = date_interval_dates_dict.popitem()
                span_code = date_interval_tuple[0]
                date_interval = date_interval_tuple[1]
                reordered_interval_dates_dict[span_code] = date_interval
                interval_dates_items = date_interval_dates_dict.items()
            return reordered_interval_dates_dict

        return date_interval_dates_dict


class DateIntervalManager:

    @staticmethod
    def createDailyIntervalDictionaryForPastYear(today_date):

        #days_between_dates = DateDelta.getDaysBetweenDateStrings(interval_start_date, today_date)
        days_between_dates = 1
        interval_count = 365
        dateIntervalDictionary = DateIntervalManager.createDateIntervalDictionaryByDays(today_date, days_between_dates, interval_count)

        return dateIntervalDictionary

    @staticmethod
    def createDateIntervalDictionaryForPastYear(today_date):

        #days_between_dates = DateDelta.getDaysBetweenDateStrings(interval_start_date, today_date)
        days_between_dates = 14
        interval_count = 26
        dateIntervalDictionary = DateIntervalManager.createDateIntervalDictionaryByDays(today_date, days_between_dates, interval_count)

        return dateIntervalDictionary

    @staticmethod
    def createDateIntervalDictionaryByDays(start_date, days_per_interval, interval_count):

        dictionary_code = start_date + '_' + str(interval_count) + '_of_' + str(days_per_interval) + '_days'
        dateIntervalDictionary = DateIntervalFactory.getDateIntervalDictionary(start_date, days_per_interval, interval_count, 'days', dictionary_code = dictionary_code)

        return dateIntervalDictionary

    @staticmethod
    def createDateIntervalDictionaries():
        """
        :return: dict
        """
        date_to_track_from = '2015-12-29'
        today_date = '2016-01-19'

        symbol_collection_span_code = 'crash_plus_12_periods_leading_up'
        days_between_dates = DateDelta.getDaysBetweenDateStrings(date_to_track_from, today_date)
        interval_count = 12
        dateIntervalDictionary = DateIntervalFactory.getDateIntervalDictionary(date_to_track_from, days_between_dates, interval_count, 'days', dictionary_code = 'leading_up_to_crash')

        since_crash_interval_code = 'since_crash'
        betweenCrashDatesInterval = DateInterval(date_to_track_from, today_date, since_crash_interval_code)
        sinceCrashDictionary = DateIntervalDictionary(since_crash_interval_code)
        sinceCrashDictionary.addDateInterval(betweenCrashDatesInterval)

        three_month_span_code = 'three_month'
        threeMonthDateInterval = DateIntervalFactory.getDateIntervals(date_to_track_from, 12, 1, 'weeks').pop()
        threeMonthDictionary = DateIntervalDictionary(three_month_span_code)
        threeMonthDictionary.addDateInterval(threeMonthDateInterval)

        one_year_span_code = 'one_year'
        oneYearDateInterval = DateIntervalFactory.getDateIntervals(date_to_track_from, 52, 1, 'weeks').pop()
        oneYearDictionary = DateIntervalDictionary(one_year_span_code)
        oneYearDictionary.addDateInterval(oneYearDateInterval)

        dateIntervalDictionaries = {symbol_collection_span_code : dateIntervalDictionary,
                                    since_crash_interval_code : sinceCrashDictionary,
                                    three_month_span_code : threeMonthDictionary,
                                    one_year_span_code : oneYearDictionary}

        return dateIntervalDictionaries
