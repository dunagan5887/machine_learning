#!/usr/bin/env python

import sys
from collections import OrderedDict
from symbolData import SymbolData
from symbolData import SymbolDataCollection
from dunagan_utility import sort_float_dictionary_ascending
from dunagan_utility import write_dictionary_to_file

stock_data_output_directory = 'data/stock_data/'

today_code = 'today'
since_delta_code = 'since_crash'
one_year_span_code = 'one_year'
three_months_span_code = 'three_months'

price_threshold = 3.0

def initializeNewSymbol(symbol, symbolCollectionInstance):
    """
    :param string symbol:
    :return: SymbolData
    """
    symbolDataInstance = symbolCollectionInstance.addSymbolToCollection(symbol)
    symbolDataInstance.initializeDeltaByCode(since_delta_code)
    symbolDataInstance.initializeSpanByCode(three_months_span_code)
    symbolDataInstance.initializeSpanByCode(one_year_span_code)
    return symbolDataInstance


last_symbol = None
last_close_price = None
symbolCollectionInstance = SymbolDataCollection()

# -----------------------------------
# BEGIN: Loop through incoming data lines
#  --------------------------------
for input_line in sys.stdin:
    input_line = input_line.strip()

    symbol_and_date, data_close_price, data_open_price, data_high_price, data_low_price, flag = input_line.split("\t", 6)
    symbol, date = symbol_and_date.split("_", 1)

    data_close_price = float(data_close_price)
    data_open_price = float(data_open_price)
    data_high_price = float(data_high_price)
    data_low_price = float(data_low_price)

    if (last_symbol != symbol):
        symbolDataInstance = initializeNewSymbol(symbol, symbolCollectionInstance)
        last_close_price = None

    if not(last_close_price is None):
        delta = data_close_price - last_close_price
        delta_percentage = delta / last_close_price
    else:
        delta = None
        delta_percentage = None

    if flag == one_year_span_code:
        symbolDataInstance.addSpanValueByCode(one_year_span_code, date, data_close_price, data_open_price, data_high_price, data_low_price, delta, delta_percentage)
    elif flag == three_months_span_code:
        symbolDataInstance.addSpanValueByCode(one_year_span_code, date, data_close_price, data_open_price, data_high_price, data_low_price, delta, delta_percentage)
        symbolDataInstance.addSpanValueByCode(three_months_span_code, date, data_close_price, data_open_price, data_high_price, data_low_price, delta, delta_percentage)
    elif flag == since_delta_code:
        symbolDataInstance.addSpanValueByCode(one_year_span_code, date, data_close_price, data_open_price, data_high_price, data_low_price, delta, delta_percentage)
        symbolDataInstance.addSpanValueByCode(three_months_span_code, date, data_close_price, data_open_price, data_high_price, data_low_price, delta, delta_percentage)
        symbolDataInstance.setDeltaBeforeByCode(since_delta_code, data_open_price)
    elif flag == today_code:
        symbolDataInstance.setDeltaAfterByCode(since_delta_code, data_close_price)
        symbolDataInstance.setTodayPrice(data_close_price)
        symbolDataInstance.addSpanValueByCode(one_year_span_code, date, data_close_price, data_open_price, data_high_price, data_low_price, delta, delta_percentage)
        symbolDataInstance.addSpanValueByCode(three_months_span_code, date, data_close_price, data_open_price, data_high_price, data_low_price, delta, delta_percentage)

    last_symbol = symbol
    last_close_price = data_close_price
# -----------------------------------
# END: Loop through incoming data lines
#  --------------------------------


sorted_symbol_price_deltas = symbolCollectionInstance.getSortedDeltaValuesByCode(since_delta_code)
symbol_price_percentage_deltas = OrderedDict()

for symbol, price_delta in sorted_symbol_price_deltas.items():
    symbolData = symbolCollectionInstance.getSymbolData(symbol)
    before_crash_price = symbolData.getDeltaBeforeByCode(since_delta_code)
    price_delta_percentage = price_delta / before_crash_price
    symbol_price_percentage_deltas[symbol] = price_delta_percentage

sorted_symbol_price_percentage_deltas = sort_float_dictionary_ascending(symbol_price_percentage_deltas)
sorted_symbol_percentage_off_three_month_average = symbolCollectionInstance.getSortedTodayPricePercentageOffSpanAveragesByCode(three_months_span_code)
sorted_symbol_percentage_off_year_average = symbolCollectionInstance.getSortedTodayPricePercentageOffSpanAveragesByCode(one_year_span_code)

sorted_symbol_percentage_off_three_month_average_above_price_threshold = symbolCollectionInstance.getSortedDictionaryOfValuesAboveTodayPriceThreshold(sorted_symbol_percentage_off_three_month_average, price_threshold)
sorted_symbol_percentage_off_year_average_above_price_threshold = symbolCollectionInstance.getSortedDictionaryOfValuesAboveTodayPriceThreshold(sorted_symbol_percentage_off_year_average, price_threshold)

write_dictionary_to_file(sorted_symbol_price_deltas, stock_data_output_directory + 'price_deltas.csv')
write_dictionary_to_file(sorted_symbol_price_percentage_deltas, stock_data_output_directory + 'percentage_deltas.csv')
write_dictionary_to_file(sorted_symbol_percentage_off_three_month_average, stock_data_output_directory + 'three_month_percentage_deltas.csv')
write_dictionary_to_file(sorted_symbol_percentage_off_three_month_average_above_price_threshold, stock_data_output_directory + 'three_month_percentage_deltas_above_threshold.csv')
write_dictionary_to_file(sorted_symbol_percentage_off_year_average, stock_data_output_directory + 'one_year_percentage_deltas.csv')
write_dictionary_to_file(sorted_symbol_percentage_off_year_average_above_price_threshold, stock_data_output_directory + 'one_year_percentage_deltas_above_threshold.csv')
