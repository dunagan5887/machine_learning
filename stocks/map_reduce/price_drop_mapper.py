#!/usr/bin/env python   
#the above just indicates to use python to intepret this file

# ---------------------------------------------------------------
#This mapper code will input a line of text and output <word, 1>
# 
# ---------------------------------------------------------------

import sys             #a python module with system functions for this OS

for line in sys.stdin:  

    line = line.strip()  #strip is a method, ie function, associated
                         #  with string variable, it will strip 
                         #   the carriage return (by default)
    data_points = line.split(',')  #split line at blanks (by default), 
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

        print( '{0}_{1}\t{2}\t{3}\t{4}\t{5}'.format(data_symbol, data_date, data_close_price, data_open_price, data_high_price, data_low_price) )
