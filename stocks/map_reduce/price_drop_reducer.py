#!/usr/bin/env python

import sys
from collections import OrderedDict
from symbolData import SymbolData
from symbolData import SymbolDataCollection
from dunagan_utility import sort_float_dictionary_ascending
from dunagan_utility import write_dictionary_to_file
from dateInterval import DateInterval
from dateInterval import DateIntervalDictionary
from dateInterval import DateIntervalFactory
from dateDelta import DateDelta

stock_data_output_directory = '/var/machine_learning/stocks/data/stock_data/'

date_to_track_from = '2015-12-29'
today_date = '2016-01-19'

symbol_collection_span_code = 'crash_plus_12_periods_leading_up'

days_between_dates = DateDelta.getDaysBetweenDateStrings(date_to_track_from, today_date)
dateIntervalDictionary = DateIntervalFactory.getDateIntervalDictionary(date_to_track_from, days_between_dates, 12, 'days', dictionary_code = 'leading_up_to_crash')

since_crash_interval_code = 'since_crash'
betweenCrashDatesInterval = DateInterval(date_to_track_from, today_date, since_crash_interval_code)
three_month_span_code = 'three_month'
threeMonthDateInterval = DateIntervalFactory.getDateIntervals(date_to_track_from, 12, 1, 'weeks').pop()
one_year_span_code = 'one_year'
oneYearDateInterval = DateIntervalFactory.getDateIntervals(date_to_track_from, 52, 1, 'weeks').pop()

auxiliaryIntervals = {since_crash_interval_code : betweenCrashDatesInterval, three_month_span_code : threeMonthDateInterval,
                      one_year_span_code : oneYearDateInterval}

price_threshold = 3.0

def initializeNewSymbol(symbol, symbolCollectionInstance):
    """
    :param string symbol:
    :return: SymbolData
    """
    symbolDataInstance = symbolCollectionInstance.addSymbolToCollection(symbol)
    symbolDataInstance.initializeSpanByCode(symbol_collection_span_code)
    for code, dateInterval in auxiliaryIntervals.items():
        symbolDataInstance.initializeSpanByCode(code)
    return symbolDataInstance


last_symbol = None
last_date = None
last_close_price = None
symbolCollectionInstance = SymbolDataCollection()

# -----------------------------------
# BEGIN: Loop through incoming data lines
#  --------------------------------

#test_file = open('/tmp/test_reduction.csv', 'r')
#incoming_data = test_file.readlines()
#test_file.close()
#for input_line in incoming_data:

for input_line in sys.stdin:
    input_line = input_line.strip()

    symbol_and_date, data_close_price, data_open_price, data_high_price, data_low_price = input_line.split("\t", 5)
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
    date_interval_codes_for_date = dateIntervalDictionary.getIntervalCodesByDate(date)
    if not(date_interval_codes_for_date is None):
        for interval_code in date_interval_codes_for_date:
            symbolDataInstance.addSpanValueByCode(symbol_collection_span_code, date_interval_codes_for_date, data_close_price, data_open_price, data_high_price, data_low_price, delta, delta_percentage)

    for interval_code, DateInterval in auxiliaryIntervals.items():
        is_date_in_interval = DateInterval.isDateInInterval(date)
        if is_date_in_interval:
            symbolDataInstance.addSpanValueByCode(interval_code, interval_code, data_close_price, data_open_price, data_high_price, data_low_price, delta, delta_percentage)

    if date == today_date:
        symbolDataInstance.setTodayPrice(data_close_price)

    last_symbol = symbol
    last_date = date
    last_close_price = data_close_price
# -----------------------------------
# END: Loop through incoming data lines
#  --------------------------------


sorted_symbol_price_deltas = symbolCollectionInstance.getSortedSpanDeltaValuesByCode(since_crash_interval_code, since_crash_interval_code)
sorted_symbol_price_delta_percentages = symbolCollectionInstance.getSortedSpanDeltaValuesByCode(since_crash_interval_code, since_crash_interval_code, get_percentages = True)

sorted_symbol_percentage_off_three_month_average = symbolCollectionInstance.getSortedTodayPricePercentageOffSpanAveragesByCode(three_month_span_code, three_month_span_code)
sorted_symbol_percentage_off_year_average = symbolCollectionInstance.getSortedTodayPricePercentageOffSpanAveragesByCode(one_year_span_code, one_year_span_code)

sorted_symbol_percentage_off_three_month_average_above_price_threshold = symbolCollectionInstance.getSortedDictionaryOfValuesAboveTodayPriceThreshold(sorted_symbol_percentage_off_three_month_average, price_threshold)
sorted_symbol_percentage_off_year_average_above_price_threshold = symbolCollectionInstance.getSortedDictionaryOfValuesAboveTodayPriceThreshold(sorted_symbol_percentage_off_year_average, price_threshold)

write_dictionary_to_file(sorted_symbol_price_deltas, stock_data_output_directory + 'price_deltas.csv')
write_dictionary_to_file(sorted_symbol_price_delta_percentages, stock_data_output_directory + 'delta_percentages.csv')
write_dictionary_to_file(sorted_symbol_percentage_off_three_month_average, stock_data_output_directory + 'three_month_percentage_deltas.csv')
write_dictionary_to_file(sorted_symbol_percentage_off_three_month_average_above_price_threshold, stock_data_output_directory + 'three_month_percentage_deltas_above_threshold.csv')
write_dictionary_to_file(sorted_symbol_percentage_off_year_average, stock_data_output_directory + 'one_year_percentage_deltas.csv')
write_dictionary_to_file(sorted_symbol_percentage_off_year_average_above_price_threshold, stock_data_output_directory + 'one_year_percentage_deltas_above_threshold.csv')
