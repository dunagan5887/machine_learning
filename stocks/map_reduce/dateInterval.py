from datetime import timedelta
from datetime import datetime
from collections import OrderedDict


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
        date_interval_dates_dict = DateIntervalFactory.getDateIntervalDates(start_date, frequency, count, unit, direction_is_past)
        dateIntervalDictionaryInstance = DateIntervalDictionary(dictionary_code)  # type: DateIntervalDictionary
        for code, date_interval_dates_dict in date_interval_dates_dict.items():
            start_date = date_interval_dates_dict[0]
            end_date = date_interval_dates_dict[1]
            dateIntervalDictionaryInstance.addDateIntervalByDates(start_date, end_date, code)
        return dateIntervalDictionaryInstance

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
            date_interval_dates_dict[code] = [datetime_to_act_from_string, interval_date_string]
            datetime_to_act_from = interval_datetime
            datetime_to_act_from_string = interval_date_string

        return date_interval_dates_dict
