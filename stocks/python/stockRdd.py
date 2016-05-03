
from dunagan_utility import DunaganListUtility
from symbolData import SymbolData
from pyspark.sql import Row

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
    def getDoesSymbolTupleHaveNoNoneValueClosure():
        """
            Returns a function closure whose functionality is described below
        :return: function closure
        """
        def doesSymbolTupleHaveNoNoneValue(symbol_tuple):
            """

            :param symbol_tuple: This is a tuple of the form (stock_symbol, list of data point tuples). These are the
                                    tuples which are returned by the function closure produced by
                                    StockRdd.getDataToClusterByDateDictionariesClosure(). They contain the data which
                                    we will use to cluster the stocks

            :return: Bool: The return value is a boolean value representing whether any of the values in the list of
                            data point tuples is a None value. None values will cause issues for the KMeans clustering
                            algorithm
            """
            # Get the list of data point tuples
            span_code_and_data_point_tuples_list = symbol_tuple[1]
            # Iterate over the data point tuples
            for span_code_and_data_point_tuple in span_code_and_data_point_tuples_list:
                data_point = span_code_and_data_point_tuple[1]
                if (data_point is None):
                    # If any of the data points are a None value, return False
                    return False
            # Return that none of the values in the list of data point tuples are None
            return True

        # Return the function closure
        return doesSymbolTupleHaveNoNoneValue

    @staticmethod
    def getConvertDataListToLineGraphDataPointKwargsClosure(dateIntervalDictionary):
        """
            This function produces a function closure described below

        :param DateIntervalDictionary dateIntervalDictionary:
        :return: function-closure
        """
        date_interval_codes = dateIntervalDictionary.getDateIntervalCodes()
        number_of_date_interval_codes = len(date_interval_codes)
        def getConvertDataListToLineGraphDataPointKwargs(cluster_data_points_list):
            """
                This function takes as input cluster_data_points_list which is a list containing data points representing
                    the percentage change in a cluster's price over a given number of time spans. This function will
                    calculate the ratio of the cluster's price at the end of each time span compared to the initial price
                    of the cluster. These values will be used to create line graphs showing the performance of the cluster
                    over a time frame. The ratio values will be used to create a kwargs dictionary. The kwargs dictionary
                    will be used to create a Row object which will be inserted into a database table

            :param list cluster_data_points_list:
            :return: dict
            """
            kwargs_dict = {}
            index = 0
            total_delta_percentage = 1.0
            for data_point in cluster_data_points_list:
                if index < number_of_date_interval_codes:
                    span_code = date_interval_codes[index]
                    delta_percentage = float(data_point)
                    total_delta_percentage = total_delta_percentage * (1.0 + delta_percentage)
                    kwargs_dict[span_code] = float(total_delta_percentage)
                index = index + 1
            return kwargs_dict
        return getConvertDataListToLineGraphDataPointKwargs


    @staticmethod
    def getConvertDataListToSpanCodeLabeledDataRowKwargsClosure(dateIntervalDictionary):
        """
            This function produces a function closure described below

        :param DateIntervalDictionary dateIntervalDictionary:
        :return: function-closure
        """
        date_interval_codes = dateIntervalDictionary.getDateIntervalCodes()
        number_of_date_interval_codes = len(date_interval_codes)
        def getConvertDataListToSpanCodeLabeledDataRowKwargs(cluster_data_points_list):
            """
                This function will produce a dictionary from a list of data points which define a cluster. The purpose of
                    this dictionary is to be used as a kwargs argument to construct a Row object

            :param list cluster_data_points_list: List of data points which define a cluster
            :return: dict - A dictionary which will represent the fields which should be used to instantiate a Row object
                                to be inserted into the database
            """
            # Initialize the dictionary
            kwargs_dict = {}
            index = 0
            for data_point in cluster_data_points_list:
                # The first `number_of_date_interval_codes` values in cluster_data_points_list will represent the
                #       percentage change in a symbol's price over a time span
                if index < number_of_date_interval_codes:
                    span_code = date_interval_codes[index]
                    kwargs_dict[span_code] = float(data_point)
                else:
                    kwargs_dict['today_off_span_average'] = float(data_point)
                index = index + 1
            return kwargs_dict
        return getConvertDataListToSpanCodeLabeledDataRowKwargs

    @staticmethod
    def getOverallDeltaPercentageForClusterClosure(dateIntervalDictionary):
        """
            This method will return a function closure which computes the overall change in a cluster's price. This
                will be computed as a ratio of end price to open price.

        :param DateIntervalDictionary dateIntervalDictionary:
        :return: function closure
        """
        # Get the date interval codes relevant to the Date Interval Dictionary
        date_interval_codes = dateIntervalDictionary.getDateIntervalCodes()
        # Get the number of date intervals in the dictionary
        number_of_date_interval_codes = len(date_interval_codes)
        def getOverallDeltaPercentageForCluster(cluster_data_points_list):
            """
                This function will compute the ratio of the cluster's end price to it's starting price. This gives us
                    a general idea of how well the stocks in this cluster performed of the entirety of the time span
                    defined by the dateIntervalDictionary passed in above

            :param list cluster_data_points_list: The list of data points which define the cluster
            :return: float - The ratio of the cluster's end price to open price. E.g. if this cluster represents a stock
                                which rose 20% over the course of the time span, this function wouldn return 1.2
            """
            total_delta_percentage = 1.0
            index = 0
            for data_point in cluster_data_points_list:
                # The first `number_of_date_interval_codes` elements in the cluster_data_points_list represent the percentage
                #   change in the cluster's price for each time interval in the dateIntervalDictionary
                if index < number_of_date_interval_codes:
                    total_delta_percentage = total_delta_percentage * (1.0 + data_point)
                index = index + 1
            return total_delta_percentage

        return getOverallDeltaPercentageForCluster

    @staticmethod
    def getDataToClusterByDateDictionariesClosure(dateIntervalDictionary):
        """
            This method will return a function closure described below

        :param DateIntervalDictionary dateIntervalDictionary: A DateIntervalDictionary object representing the time-frame
                                                                that we want to cluster stocks over
        :return: function
        """
        date_interval_codes = dateIntervalDictionary.getDateIntervalCodes()
        def getDataToClusterByDateDictionaries(symbolInstance_key_tuple):
            """
                This method will return a tuple. This tuple will map a stock's symbol to a list of data points which will
                    be used to cluster stocks by. The data points will be the percentage by which a stock's price rose or
                    fell from the opening of the first day in a time span to the closing of the last day in the time span
                    for each time span defined in dateIntervalDictionary. There will also be a data point representing the
                    ratio of the stocks' "Today" price to the average closing price of the most recent time frame

            :param tuple symbolInstance_key_tuple: A tuple of the form (symbol, symbolDataInstance)

            :return: tuple (symbol_code, list of tuples (data_point_description, data_point_value))
            """
            symbol_code = symbolInstance_key_tuple[0]
            symbolDataInstance = symbolInstance_key_tuple[1]

            # Build a list of tuples containing the points we will want to use to cluster the stocks
            data_list = []
            span_today_price_off_average = None

            # For each time span contained in dateIntervalDictionary
            for span_code in date_interval_codes:

                # Get the symbol's Span object for the specified span_code
                spanInstance = symbolDataInstance.getSpanByCode(span_code)
                # Get the percentage rise/fall in the stock's price from the opening price of the first day in the span
                #   to the closing price of the last day of the span
                span_delta_percentage = spanInstance.getSpanDelta(get_percentage_delta = True)
                # Create a tuple containing the span code and the percentage rise/fall and add it to the list
                span_code_and_delta_percentage_tuple = (span_code, span_delta_percentage)
                data_list.append(span_code_and_delta_percentage_tuple)

                # One of the data points we want is the stocks' "Today" price divided by the average closing price of
                #   the most recent span. The span codes in date_interval_codes should be ordered with the most recent
                #   span first
                span_average = spanInstance.getSpanCloseAverage()
                if not(span_average is None) and (span_today_price_off_average is None):
                    # Add the span_today_price_off_average value to our list of data point tuples
                    span_today_price_off_average = symbolDataInstance.getTodayPriceOffSpanAverage(span_code)
                    span_today_price_off_average_tuple = ('span_today_price_off_average', span_today_price_off_average)
                    data_list.append(span_today_price_off_average_tuple)

            # Return the tuple mapping the stock's symbol to the list of data-points tuples
            return (symbol_code, data_list)

        # Return the function closure
        return getDataToClusterByDateDictionaries

    @staticmethod
    def getDownStocksDataListClosure(today_date):
        """
        :return: function
        """
        def getDownStocksDataList(symbol_and_instance_tuple): # type: SymbolData
            """
            :param SymbolData symbolInstance:
            :return: tuple
            """
            symbol = symbol_and_instance_tuple[0]
            symbolInstance = symbol_and_instance_tuple[1]
            # Remove today's date from the getMinSpanUnitDelta calculation
            min_span_unit_delta_percentage = symbolInstance.getMinSpanUnitDelta(get_percentage_delta = True)
            today_unit_delta_percentage = symbolInstance.getTodayDeltaPercentage()
            today_price = symbolInstance.getTodayPrice()
            if today_unit_delta_percentage >= 0.0:
                return (symbol, None, today_price, today_unit_delta_percentage)
            if min_span_unit_delta_percentage >= 0.0:
                return (symbol, float("inf"), today_price, today_unit_delta_percentage)
            span_unit_delta_percentage_ratio = today_unit_delta_percentage / min_span_unit_delta_percentage
            return (symbol, span_unit_delta_percentage_ratio, today_price, today_unit_delta_percentage)
        return getDownStocksDataList

    @staticmethod
    def getSymbolDataInstanceForDateDictionaryDataPointsClosure(dateIntervalDictionary, today_date):
        """
            This method returns a function closure whose objective is detailed below. Due to lexical scoping, the function
                closure will respect the values which are defined for variables dateIntervalDictionary and today_date
                above

        :param dict dateIntervalDictionaries: A dictionary of date intervals which will represent the spans of time which
                                                comprise the time-frame

        :param string today_date: The date to treat as "Today's Date"
        :return: function closure
        """
        def getSymbolDataInstanceForDateDictionaryDataPoints(symbol_key_list_tuple):
            """
            :param list symbol_data_list: This is a list whose elements are tuples of the form (Key, Value) where each
                                            Key is a stock symbol and each Value is a list of comma-delimited strings,
                                            each representing a date's worth of stock data

            :return: tuple (key, value)|None: The returned key is a stock's symbol while the returned value is an object of
                                                type SymbolData. The symbol data object will contain the data which was
                                                contained in the list of comma-delimited strings. However if the list
                                                is empty, a value of None will be returned
            """

            # Index 1 of the symbol_key_list_tuple contains the list of comma-delimited strings
            symbol_data_list = symbol_key_list_tuple[1]
            if len(symbol_data_list) < 1:
                # If the stock had no data points within the time-frame specified by dateIntervalDictionary, return a
                #    None value
                return SymbolData(None)

            # Index 0 of the symbol_key_list_tuple contains the stock symbol
            symbol_code = symbol_key_list_tuple[0]
            # Instantiate the SymbolData object
            symbolDataInstance = SymbolData(symbol_code)

            # Iterate over the strings of stock-date data in the list
            for symbol_date_data in symbol_data_list:

                # We have an eventual data point which will operate on the stock's close price and percentage delta,
                #       so record those values if this symbol_date_data string is specific to today's date
                date = symbol_date_data[StockRdd.DATE_INDEX]
                if date == today_date:
                    symbolDataInstance.setTodayPrice(symbol_date_data[StockRdd.CLOSE_PRICE_INDEX])
                    symbolDataInstance.setTodayDeltaPercentage(symbol_date_data[StockRdd.DELTA_PERCENTAGE_INDEX])

                # Get all of the time spans which the date for this row belongs to
                date_interval_codes_for_date = dateIntervalDictionary.getIntervalCodesByDate(date)
                if not(date_interval_codes_for_date is None):
                    for date_interval_code in date_interval_codes_for_date:
                        # Add a value row for each span that this date belongs to
                        symbolDataInstance.addSpanValueByCode(date_interval_code, date_interval_code, symbol_date_data[StockRdd.CLOSE_PRICE_INDEX],
                                                                  symbol_date_data[StockRdd.OPEN_PRICE_INDEX], symbol_date_data[StockRdd.HIGH_PRICE_INDEX],
                                                                  symbol_date_data[StockRdd.LOW_PRICE_INDEX], symbol_date_data[StockRdd.DELTA_INDEX],
                                                                  symbol_date_data[StockRdd.DELTA_PERCENTAGE_INDEX])

            # Return a tuple comprised of the stock symbol and the SymbolData object
            return (symbol_code, symbolDataInstance)

        # Return the function closure
        return getSymbolDataInstanceForDateDictionaryDataPoints

    @staticmethod
    def sort_and_compute_deltas(symbol_data_list):
        """
            This method will take an unsorted list of stock data, with each value in the list being a string starting
                with a stock symbol and a price date. This method will first sort the list of data. It will then compute
                the price (and percentage) rise/fall in the stock's price for each date in the list

        :param symbol_data_list: List of csv data lines all pertaining to a particular stock
                                    Each line contains a day's worth of stock price data
                                    These lines can not be assumed to be sorted in any particular order

        :return: Returns a modified version of the symbol_data_list passed in. Each value in the list will be updated
                    to include the price (and percentage) rise/fall in the stocks' price for each date in the list. The
                    list will also be sorted by date ascending
        """
        # Sort the list of data in ascending order
        symbol_data_list.sort()
        # Convert the csv data lines to be arrays of data
        symbol_data_list = map(lambda line_to_split : line_to_split.split(','), symbol_data_list)

        last_close_price = None
        index = 0
        # Iterating over the list of date data points
        for stock_date_data_array in symbol_data_list:
            # Get the closing price from the data list
            data_close_price = float(stock_date_data_array[StockRdd.CLOSE_PRICE_INDEX])
            # The close price shouldn't be None, but check just to be sure
            if not(last_close_price is None) :
                # Compute the change in price for the day
                delta = data_close_price - last_close_price
                # Compute the percentage change in price for the day
                if last_close_price != 0.0:
                    delta_percentage = delta / last_close_price
                else:
                    delta_percentage = 100.0
            else:
                delta = None
                delta_percentage = None
            # Add the stock price change and percentage change to the data list
            stock_date_data_array.append(delta)
            stock_date_data_array.append(delta_percentage)
            # Replace the original list with the updated list
            symbol_data_list[index] = stock_date_data_array
            # Keep track of this close price to compute the delta for the next day
            last_close_price = data_close_price
            index += 1

        # Return the updated list of data
        return symbol_data_list


    @staticmethod
    def getMapStockCsvToKeyValueForDatesInDictionaryClosure(dateIntervalDictionary):
        """
        This method constructs a function closure mapStockCsvToKeyValue(). The purpose of that function closure is to
            convert lines of the following format

            Date,Open,High,Low,Close,Volume,Adj Close,Stock_Symbol

            into a (Key, Value) tuple pair which maps the Stock Symbol as the key to the following string of data points

            Stock_Symbol,Date,Close,Open,High,Low

            The function closure will filter out data from dates which reside outside of the time-frame defined by
                dateIntervalDictionary. This is done via lexical scoping as the function closure will define the values
                earliest_date, latest_date as whatever values the variables contained at the time the function closure
                was created

        :param DateIntervalDictionary dateIntervalDictionary:
        :return: function()
        """
        earliest_date = dateIntervalDictionary.getEarliestDateInDictionary()
        latest_date = dateIntervalDictionary.getLatestDateInDictionary()

        def mapStockCsvToKeyValue(line_of_data):
            """
            :param string line_of_data: CSV line of the form Date,Open,High,Low,Close,Volume,Adj Close,Stock_Symbol
            :return: tuple (string, list)| None
            """

            # The file delimiters are commas
            data_points = line_of_data.split(',')

            # If this row is a date outside of our desired time-frame, ignore it
            data_date = data_points[0]
            if (data_date < earliest_date) or (data_date > latest_date):
                return None

            # Ensure this row was not a csv file header
            if (data_date != 'Date'):
                data_close_price = data_points[4]
                data_open_price = data_points[1]
                data_high_price = data_points[2]
                data_low_price = data_points[3]
                data_symbol = data_points[7]

                key = data_symbol
                value = '{0},{1},{2},{3},{4},{5}'.format(data_symbol, data_date, data_close_price, data_open_price, data_high_price, data_low_price)
                # Return the Key, Value pair tuple
                return (key , [value])
            # If this is a csv file header row, ignore it
            return None

        # Return the function closure we just created above
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