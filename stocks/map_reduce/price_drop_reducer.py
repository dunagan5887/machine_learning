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
symbolCollectionInstance = SymbolDataCollection()

# -----------------------------------
# Loop through incoming data lines
#  --------------------------------
for input_line in sys.stdin:
    input_line = input_line.strip()

    symbol_and_date, price, flag = input_line.split("\t", 2)
    symbol, date = symbol_and_date.split("_", 1)

    price = float(price)

    if (last_symbol != symbol):
        symbolDataInstance = initializeNewSymbol(symbol, symbolCollectionInstance)

    if flag == one_year_span_code:
        symbolDataInstance.addSpanValueByCode(one_year_span_code, price)
    elif flag == three_months_span_code:
        symbolDataInstance.addSpanValueByCode(one_year_span_code, price)
        symbolDataInstance.addSpanValueByCode(three_months_span_code, price)
    elif flag == since_delta_code:
        symbolDataInstance.addSpanValueByCode(one_year_span_code, price)
        symbolDataInstance.addSpanValueByCode(three_months_span_code, price)
        symbolDataInstance.setDeltaBeforeByCode(since_delta_code, price)
    elif flag == today_code:
        symbolDataInstance.setDeltaAfterByCode(since_delta_code, price)
        symbolDataInstance.setTodayPrice(price)
        symbolDataInstance.addSpanValueByCode(one_year_span_code, price)
        symbolDataInstance.addSpanValueByCode(three_months_span_code, price)

    last_symbol = symbol


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
