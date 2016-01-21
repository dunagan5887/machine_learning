#!/usr/bin/env python

# ---------------------------------------------------------------
#This reducer code will input a line of text and 
#    output <word, total-count>
# ---------------------------------------------------------------
import sys

def get_percentage_delta_off_average(list_of_prices, compare_price):
    list_count = len(list_of_prices)
    list_sum = float(sum(list_of_prices))
    list_average = list_sum / list_count
    delta = compare_price - list_average
    drop_percentage = delta / list_average
    return drop_percentage

def reset_symbol_data():
    symbol_since_total = 0.0
    last_today_price = 0.0
    last_start_price = 0.0
    three_month_prices = list()
    one_year_prices = list()

last_symbol      = None              #initialize these variables
symbol_since_total = 0.0
last_today_price = 0.0
last_start_price = 0.0
three_month_prices = list()
one_year_prices = list()

price_deltas_by_symbol = dict()
percentage_deltas_by_symbol = dict()
three_month_average_drop_by_symbol = dict()
one_year_average_drop_by_symbol = dict()

no_last_start_price_file = open('no_last_start_price.csv', 'w')

# -----------------------------------
# Loop thru file
#  --------------------------------
for input_line in sys.stdin:
    input_line = input_line.strip()

    symbol, price, flag = input_line.split("\t", 2)
    
    price = float(price)
    
    if (last_symbol and (last_symbol != symbol)):
        price_deltas_by_symbol[last_symbol] = symbol_since_total
        
        if last_start_price != 0:
            percentage_delta = symbol_since_total / last_start_price
            percentage_deltas_by_symbol[last_symbol] = percentage_delta
            # Compute the three month average percentage delta
            three_month_drop_percentage = get_percentage_delta_off_average(three_month_prices, last_today_price)
            three_month_average_drop_by_symbol[last_symbol] = three_month_drop_percentage
            # Compute the one year average percentage delta
            one_year_drop_percentage = get_percentage_delta_off_average(one_year_prices, last_today_price)
            one_year_average_drop_by_symbol[symbol] = one_year_drop_percentage
        else:
            no_last_start_price_file.write(last_symbol)
        
        symbol_since_total = 0.0
        last_today_price = 0.0
        last_start_price = 0.0
        three_month_prices = list()
        one_year_prices = list()

    if flag == 'since':
        symbol_since_total -= price
        last_start_price = price
    elif flag == 'today':
        symbol_since_total += price
        last_today_price = price
    elif flag == 'one_year':
        one_year_prices.append(price)
    elif flag == 'three_months':
        three_month_prices.append(price)

    last_symbol = symbol

if symbol_since_total != 0.0:
    price_deltas_by_symbol[last_symbol] = symbol_since_total
    if last_start_price != 0:
        percentage_delta = symbol_since_total / last_start_price
        percentage_deltas_by_symbol[last_symbol] = percentage_delta
        # Compute the three month average percentage delta
        three_month_delta_percentage = get_percentage_delta_off_average(three_month_prices, last_today_price)
        three_month_average_drop_by_symbol[last_symbol] = three_month_delta_percentage
        # Compute the one year average percentage delta
        one_year_delta_percentage = get_percentage_delta_off_average(one_year_prices, last_today_price)
        one_year_average_drop_by_symbol[symbol] = one_year_delta_percentage
    else:
        no_last_start_price_file.write('{0}\n'.format(last_symbol))

no_last_start_price_file.close()
            
sorted_symbol_prices = sorted(price_deltas_by_symbol, key = lambda i: float(price_deltas_by_symbol[i]), reverse = False)
sorted_symbol_percentages = sorted(percentage_deltas_by_symbol, key = lambda i: float(percentage_deltas_by_symbol[i]), reverse = False)
sorted_symbol_three_month_deltas = sorted(three_month_average_drop_by_symbol, key = lambda i: (three_month_average_drop_by_symbol[i]), reverse = False)
sorted_symbol_one_year_deltas = sorted(one_year_average_drop_by_symbol, key = lambda i: (one_year_average_drop_by_symbol[i]), reverse = False)

price_file = open('price_deltas.csv', 'w')
for symbol in sorted_symbol_prices:
    price_file.write( '{0}\t{1}\n'.format(symbol, price_deltas_by_symbol[symbol]) )
price_file.close()

percentage_file = open('percentage_deltas.csv', 'w')
for symbol in sorted_symbol_percentages:
    percentage_file.write( '{0}\t{1}\n'.format(symbol, percentage_deltas_by_symbol[symbol]) )
percentage_file.close()

three_month_file = open('three_month_percentage_deltas.csv', 'w')
for symbol in sorted_symbol_three_month_deltas:
    three_month_file.write( '{0}\t{1}\n'.format(symbol, three_month_average_drop_by_symbol[symbol]) )
three_month_file.close()
    
one_year_file = open('one_year_percentage_deltas.csv', 'w')
for symbol in sorted_symbol_one_year_deltas:
    one_year_file.write( '{0}\t{1}\n'.format(symbol, one_year_average_drop_by_symbol[symbol]) )
one_year_file.close()
