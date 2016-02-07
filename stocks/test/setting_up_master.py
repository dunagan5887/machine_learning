#A script to execute kmeans clustering in spark
#to run enter: >>> exec(open("./dokmeans.py").read())

import sys
import numpy as np
from pyspark.mllib.clustering import KMeans
from pyspark import SparkContext

sc = SparkContext("spark://ubuntu:7077", "Stock Clustering", pyFiles=[])

from dateDelta import DateDelta
from dateInterval import DateInterval
from dateInterval import DateIntervalFactory
from dunagan_utility import write_dictionary_to_file
from symbolData import SymbolDataCollection


from clusterHelper import ClusterHelper



stock_data_output_directory = '/var/machine_learning/stocks/data/stock_data/'

date_to_track_from = '2015-12-29'
today_date = '2016-01-19'

symbol_collection_span_code = 'crash_plus_12_periods_leading_up'

days_between_dates = DateDelta.getDaysBetweenDateStrings(date_to_track_from, today_date)
interval_count = 12
dateIntervalDictionary = DateIntervalFactory.getDateIntervalDictionary(date_to_track_from, days_between_dates, interval_count, 'days', dictionary_code = 'leading_up_to_crash')

since_crash_interval_code = 'since_crash'
betweenCrashDatesInterval = DateInterval(date_to_track_from, today_date, since_crash_interval_code)
three_month_span_code = 'three_month'
threeMonthDateInterval = DateIntervalFactory.getDateIntervals(date_to_track_from, 12, 1, 'weeks').pop()
one_year_span_code = 'one_year'
oneYearDateInterval = DateIntervalFactory.getDateIntervals(date_to_track_from, 52, 1, 'weeks').pop()

auxiliaryIntervals = {since_crash_interval_code : betweenCrashDatesInterval, three_month_span_code : threeMonthDateInterval,
                      one_year_span_code : oneYearDateInterval}

price_threshold = 3.0
past_year_days_threshold = float(days_between_dates) * (5.0/7.0) * interval_count

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

test_file = open('/tmp/historical_stock_data.csv', 'r')
incoming_data = test_file.readlines()
test_file.close()
for input_line in incoming_data:

#for input_line in sys.stdin:
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

        if last_close_price == 0.0:
            delta_percentage = 100.0
        else:
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


print "\n\nAbout to broadcast\n\n\n"


broadcastSymbolCollectionInstance = sc.broadcast(symbolCollectionInstance)

print broadcastSymbolCollectionInstance.value
print dir(broadcastSymbolCollectionInstance)

