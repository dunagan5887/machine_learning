
from dunagan_utility import DunaganListUtility


# NEED_UNIT_TESTS

class StockRdd:

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
    def mapStockCsvToKeyValue(line_of_data):
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

            return (key , value)

        return None

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