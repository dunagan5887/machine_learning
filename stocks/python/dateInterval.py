from datetime import timedelta
from datetime import datetime
from collections import OrderedDict
from dateDelta import DateDelta


class DateInterval:
    
    def __init__(self, start_date, end_date, code = None):
        self.start_date = start_date
        self.end_date = end_date
        self.code = code

    def isDateInInterval(self, date_to_compare):
        """
        :param string date_to_compare:
        :return: bool
        """
        return ((date_to_compare >= self.start_date) and (date_to_compare <= self.end_date))


class DateIntervalDictionary:
    
    def __init__(self, dictionary_code):
        """
        :param string dictionary_code:
        :return:
        """
        self.dictionary_code = dictionary_code
        self.date_interval_dictionary = {}
        self.earliest_date = None
        self.latest_date = None

    def getEarliestDateInDictionary(self):
        return self.earliest_date

    def getLatestDateInDictionary(self):
        return self.latest_date

    def getDateIntervalCodes(self):
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

        return self

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
            code_label = str(i * frequency) + '-' + str((i+1) * frequency)
            code = code_label + '_' + str(unit)
            if direction_is_past:
                date_interval_dates_dict[code] = [interval_date_string, datetime_to_act_from_string]
            else:
                date_interval_dates_dict[code] = [datetime_to_act_from_string, interval_date_string]
            datetime_to_act_from = interval_datetime
            datetime_to_act_from_string = interval_date_string

        return date_interval_dates_dict


class DateIntervalManager:

    @staticmethod
    def createDateIntervalDictionaryForPastYear():
        date_to_track_from = '2015-12-19'
        today_date = '2016-01-19'

        days_between_dates = DateDelta.getDaysBetweenDateStrings(date_to_track_from, today_date)
        interval_count = 12
        dateIntervalDictionary = DateIntervalFactory.getDateIntervalDictionary(today_date, days_between_dates, interval_count, 'days', dictionary_code = 'leading_up_to_crash')

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
