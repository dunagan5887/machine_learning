from datetime import timedelta
import datetime
import time


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
    def getDateIntervalDates(start_date, frequency, count, unit = None, direction_is_past = None):
        """
        :param string start_date: %Y-%m-%d format required
        :param int frequency:
        :param int count:
        :param string unit:
        :param bool direction_is_past:
        :return: dict
        """
        if (unit is None):
            unit = "weeks"
        if (direction_is_past is None):
            direction_is_past = True
        date_interval_dates_dict = {}
        date_interval_dates_dict['initial'] = start_date
        datetime_to_act_from = time.strptime(start_date, '%Y-%m-%d')
        for i in range(1, count):
            method_arguments_dict = {unit: frequency}
            time_delta = timedelta(**method_arguments_dict)
            interval_datetime = datetime_to_act_from - time_delta if direction_is_past else datetime_to_act_from + time_delta
            interval_date_string = interval_datetime.strftime('%Y-%m-%d')
            code_label = i * frequency
            code = code_label + '_' + unit
            date_interval_dates_dict[code] = [datetime_to_act_from, interval_date_string]

        return date_interval_dates_dict
