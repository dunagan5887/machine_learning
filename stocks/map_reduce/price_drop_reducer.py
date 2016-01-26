#!/usr/bin/env python

import sys
from collections import OrderedDict
from symbolData import SymbolData
from symbolData import SymbolDataCollection
from dunagan_utility import sort_float_dictionary_ascending
from dunagan_utility import write_dictionary_to_file

stock_data_output_directory = 'data/stock_data/'

span_code = 'year_leading_to_crash_and_since'
today_code = 'today'
since_crash_code = 'since_crash'
one_year_span_code = 'one_year'
three_months_span_code = 'three_months'

price_threshold = 3.0

def initializeNewSymbol(symbol, symbolCollectionInstance):
    """
    :param string symbol:
    :return: SymbolData
    """
    symbolDataInstance = symbolCollectionInstance.addSymbolToCollection(symbol)
    symbolDataInstance.initializeSpanByCode(span_code)
    return symbolDataInstance


last_symbol = None
last_date = None
last_close_price = None
symbolCollectionInstance = SymbolDataCollection()

# -----------------------------------
# BEGIN: Loop through incoming data lines
#  --------------------------------
#for input_line in sys.stdin:
#test_file = open('/tmp/test_reduction.csv', 'r')
#incoming_data = test_file.readlines()
#test_file.close()

for input_line in sys.stdin:
    input_line = input_line.strip()

    symbol_and_date, data_close_price, data_open_price, data_high_price, data_low_price, flag = input_line.split("\t", 6)
    symbol, date = symbol_and_date.split("_", 1)

    if date == last_date:
        # We don't want to add the same date multiple times
        continue

    data_close_price = float(data_close_price)
    data_open_price = float(data_open_price)
    data_high_price = float(data_high_price)
    data_low_price = float(data_low_price)

    if (last_symbol != symbol):
        symbolDataInstance = initializeNewSymbol(symbol, symbolCollectionInstance)
        last_close_price = None
        last_date = None

    if (not(last_close_price is None) and (not(last_date) is None)):
        delta = data_close_price - last_close_price
        delta_percentage = delta / last_close_price
    else:
        delta = None
        delta_percentage = None

    unit_labels = []

    if flag == one_year_span_code:
        unit_labels = [one_year_span_code]
    elif flag == three_months_span_code:
        unit_labels = [one_year_span_code, three_months_span_code]
    elif flag == since_crash_code:
        unit_labels = [one_year_span_code, three_months_span_code, since_crash_code]
    elif flag == today_code:
        unit_labels = [one_year_span_code, three_months_span_code, since_crash_code]
        symbolDataInstance.setTodayPrice(data_close_price)

    symbolDataInstance.addSpanValueByCode(span_code, unit_labels, data_close_price, data_open_price, data_high_price, data_low_price, delta, delta_percentage)

    last_symbol = symbol
    last_date = date
    last_close_price = data_close_price
# -----------------------------------
# END: Loop through incoming data lines
#  --------------------------------


sorted_symbol_price_deltas = symbolCollectionInstance.getSortedSpanDeltaValuesByCode(span_code, since_crash_code)
sorted_symbol_price_delta_percentages = symbolCollectionInstance.getSortedSpanDeltaValuesByCode(span_code, since_crash_code, get_percentages = True)

sorted_symbol_percentage_off_three_month_average = symbolCollectionInstance.getSortedTodayPricePercentageOffSpanAveragesByCode(span_code, three_months_span_code)
sorted_symbol_percentage_off_year_average = symbolCollectionInstance.getSortedTodayPricePercentageOffSpanAveragesByCode(span_code, one_year_span_code)

sorted_symbol_percentage_off_three_month_average_above_price_threshold = symbolCollectionInstance.getSortedDictionaryOfValuesAboveTodayPriceThreshold(sorted_symbol_percentage_off_three_month_average, price_threshold)
sorted_symbol_percentage_off_year_average_above_price_threshold = symbolCollectionInstance.getSortedDictionaryOfValuesAboveTodayPriceThreshold(sorted_symbol_percentage_off_year_average, price_threshold)

write_dictionary_to_file(sorted_symbol_price_deltas, stock_data_output_directory + 'price_deltas.csv')
write_dictionary_to_file(sorted_symbol_price_delta_percentages, stock_data_output_directory + 'delta_percentages.csv')
write_dictionary_to_file(sorted_symbol_percentage_off_three_month_average, stock_data_output_directory + 'three_month_percentage_deltas.csv')
write_dictionary_to_file(sorted_symbol_percentage_off_three_month_average_above_price_threshold, stock_data_output_directory + 'three_month_percentage_deltas_above_threshold.csv')
write_dictionary_to_file(sorted_symbol_percentage_off_year_average, stock_data_output_directory + 'one_year_percentage_deltas.csv')
write_dictionary_to_file(sorted_symbol_percentage_off_year_average_above_price_threshold, stock_data_output_directory + 'one_year_percentage_deltas_above_threshold.csv')
