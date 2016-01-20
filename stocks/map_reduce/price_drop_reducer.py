#!/usr/bin/env python

# ---------------------------------------------------------------
#This reducer code will input a line of text and 
#    output <word, total-count>
# ---------------------------------------------------------------
import sys

last_symbol      = None              #initialize these variables
symbol_total = 0.0
last_start_price = 0.0

price_deltas_by_symbol = dict()
percentage_deltas_by_symbol = dict()

no_last_start_price_file = open('no_last_start_price.csv', 'w')

# -----------------------------------
# Loop thru file
#  --------------------------------
for input_line in sys.stdin:
    input_line = input_line.strip()

    symbol, price, flag = input_line.split("\t", 2)
    
    price = float(price)
    
    if (last_symbol and (last_symbol != symbol)):
        price_deltas_by_symbol[last_symbol] = symbol_total
        
        if last_start_price != 0:
            percentage_delta = symbol_total / last_start_price
            percentage_deltas_by_symbol[last_symbol] = percentage_delta
        else:
            no_last_start_price_file.write(last_symbol)
        
        symbol_total = 0.0
        last_start_price = 0.0

    if flag == 'since':
        symbol_total -= price
        last_start_price = price
    else:
        symbol_total += price

    last_symbol = symbol

if symbol_total != 0.0:
    price_deltas_by_symbol[last_symbol] = symbol_total
    if last_start_price != 0:
        percentage_delta = symbol_total / last_start_price
        percentage_deltas_by_symbol[last_symbol] = percentage_delta
    else:
        no_last_start_price_file.write(last_symbol)

            

sorted_symbol_prices = sorted(price_deltas_by_symbol, key = lambda i: float(price_deltas_by_symbol[i]), reverse = False)
sorted_symbol_percentages = sorted(percentage_deltas_by_symbol, key = lambda i: float(percentage_deltas_by_symbol[i]), reverse = False)

price_file = open('price_deltas.csv', 'w')
for symbol in sorted_symbol_prices:
    price_file.write( '{0}\t{1}\n'.format(symbol, price_deltas_by_symbol[symbol]) )
price_file.close()

percentage_file = open('percentage_deltas.csv', 'w')
for symbol in sorted_symbol_percentages:
    percentage_file.write( '{0}\t{1}\n'.format(symbol, percentage_deltas_by_symbol[symbol]) )
percentage_file.close()

no_last_start_price_file.close()
    
    
    
    
    
    