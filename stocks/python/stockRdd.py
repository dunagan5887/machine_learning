
from dunagan_utility import DunaganListUtility
from symbolData import SymbolData

# NEED_UNIT_TESTS

class StockRdd:

    SYMBOL_INDEX = 0
    DATE_INDEX = 1
    CLOSE_PRICE_INDEX = 2
    OPEN_PRICE_INDEX = 3
    HIGH_PRICE_INDEX = 4
    LOW_PRICE_INDEX = 5
    DELTA_INDEX = 6
    DELTA_PERCENTAGE_INDEX = 7

    @staticmethod
    def getDataToClusterByDateDictionariesClosure(dateIntervalDictionary):
        """
        :param DateIntervalDictionary dateIntervalDictionary:
        :return: function
        """

        date_interval_codes = dateIntervalDictionary.getDateIntervalCodes()

        def getDataToClusterByDateDictionaries(symbolInstance_key_tuple):
            """
            :param SymbolData symbolDataInstance:
            :return: list
            """
            symbol_code = symbolInstance_key_tuple[0]
            symbolDataInstance = symbolInstance_key_tuple[1]

            data_list = []
            span_today_price_off_average = None
            for span_code in date_interval_codes:
                spanInstance = symbolDataInstance.getSpanByCode(span_code)
                span_delta_percentage = spanInstance.getSpanDelta(get_percentage_delta = True)
                span_range = spanInstance.getSpanPriceRange()
                data_list.append(span_delta_percentage)
                data_list.append(span_range)

                span_average = spanInstance.getSpanCloseAverage()
                if not(span_average is None) and (span_today_price_off_average is None):
                    span_today_price_off_average = symbolDataInstance.getTodayPriceOffSpanAverage(span_code)

            data_list.append(span_today_price_off_average)

            return (symbol_code, data_list)

        return getDataToClusterByDateDictionaries

    @staticmethod
    def getSymbolDataInstanceForDateDictionaryDataPointsClosure(dateIntervalDictionary, today_date):
        """
        :param dict dateIntervalDictionaries:
        :param string today_date:
        :return: function
        """
        def getSymbolDataInstanceForDateDictionaryDataPoints(symbol_key_list_tuple):
            """
            :param list symbol_data_list:
            :return: tuple (key, value)
            """
            symbol_data_list = symbol_key_list_tuple[1]
            if len(symbol_data_list) < 1:
                return SymbolData(None)

            symbol_code = symbol_key_list_tuple[0]
            symbolDataInstance = SymbolData(symbol_code)

            for symbol_date_data in symbol_data_list:
                date = symbol_date_data[StockRdd.DATE_INDEX]

                if date == today_date:
                    symbolDataInstance.setTodayPrice(symbol_date_data[StockRdd.CLOSE_PRICE_INDEX])

                date_interval_codes_for_date = dateIntervalDictionary.getIntervalCodesByDate(date)

                if not(date_interval_codes_for_date is None):
                    for date_interval_code in date_interval_codes_for_date:
                        symbolDataInstance.addSpanValueByCode(date_interval_code, date_interval_code, symbol_date_data[StockRdd.CLOSE_PRICE_INDEX],
                                                                  symbol_date_data[StockRdd.OPEN_PRICE_INDEX], symbol_date_data[StockRdd.HIGH_PRICE_INDEX],
                                                                  symbol_date_data[StockRdd.LOW_PRICE_INDEX], symbol_date_data[StockRdd.DELTA_INDEX],
                                                                  symbol_date_data[StockRdd.DELTA_PERCENTAGE_INDEX])

            return (symbol_code, symbolDataInstance)

        return getSymbolDataInstanceForDateDictionaryDataPoints

    @staticmethod
    def sort_and_compute_deltas(symbol_data_list):
        symbol_data_list.sort()
        symbol_data_list = map(lambda line_to_split : line_to_split.split(','), symbol_data_list)

        last_close_price = None
        index = 0
        for stock_date_data_array in symbol_data_list:
            data_close_price = float(stock_date_data_array[2])
            if not(last_close_price is None) :
                delta = data_close_price - last_close_price

                if last_close_price != 0.0:
                    delta_percentage = delta / last_close_price
                else:
                    delta_percentage = 100.0
            else:
                delta = None
                delta_percentage = None


            stock_date_data_array.append(delta)
            stock_date_data_array.append(delta_percentage)
            symbol_data_list[index] = stock_date_data_array

            last_close_price = data_close_price
            index += 1

        return symbol_data_list


    @staticmethod
    def getMapStockCsvToKeyValueForDatesInDictionaryClosure(dateIntervalDictionary):
        """
        :param DateIntervalDictionary dateIntervalDictionary:
        :return:function
        """
        earliest_date = dateIntervalDictionary.getEarliestDateInDictionary()
        latest_date = dateIntervalDictionary.getLatestDateInDictionary()

        def mapStockCsvToKeyValue(line_of_data):
            """
            :param string line_of_data:
            :return: tuple (string, list)
            """
            data_points = line_of_data.split(',')  #split line at blanks (by default),
                                 #   and return a list of keys

            # Rows of data_points have the following header:
            #       Date,Open,High,Low,Close,Volume,Adj Close,T

            # If this is a header row, ignore it
            data_date = data_points[0]

            if (data_date < earliest_date) or (data_date > latest_date):
                return None

            if (data_date != 'Date'):
                data_close_price = data_points[4]
                data_open_price = data_points[1]
                data_high_price = data_points[2]
                data_low_price = data_points[3]
                data_symbol = data_points[7]

                key = data_symbol
                value = '{0},{1},{2},{3},{4},{5}'.format(data_symbol, data_date, data_close_price, data_open_price, data_high_price, data_low_price)

                return (key , [value])

            return None

        return mapStockCsvToKeyValue


    @staticmethod
    def mapStockCsvForSort(line_of_data):
        line = line_of_data.strip()  #strip is a method, ie function, associated
                         #  with string variable, it will strip
                         #   the carriage return (by default)
        data_points = line_of_data.split(',')  #split line at blanks (by default),
                             #   and return a list of keys

        # Rows of data_points have the following header:
        #       Date,Open,High,Low,Close,Volume,Adj Close,T

        # If this is a header row, ignore it
        data_date = data_points[0]

        if (data_date != 'Date'):
            data_close_price = data_points[4]
            data_open_price = data_points[1]
            data_high_price = data_points[2]
            data_low_price = data_points[3]
            data_symbol = data_points[7]

            key = data_symbol
            value = '{0},{1},{2},{3},{4},{5}'.format(data_symbol, data_date, data_close_price, data_open_price, data_high_price, data_low_price)

            return value
        return None

    @staticmethod
    def convertLineToDict(mapped_line):
        data_points = mapped_line.split(",")
        symbol = data_points[0]
        return {symbol : [mapped_line]}

    @staticmethod
    def reduceData(value_a, value_b):
        keys_a = value_a.keys()
        keys_b = value_b.keys()

        keys_both = keys_a + keys_b
        keys_both = set(keys_both)

        for symbol in keys_both:
            list_a = value_a[symbol] if symbol in value_a else []
            list_b = value_b[symbol] if symbol in value_b else []
            list_both = list_a + list_b
            value_a[symbol] = list_both

        return value_a


    @staticmethod
    def getSymbolForMappedLine(mapped_line):
        data_points = mapped_line.split("\t")
        return data_points[0]