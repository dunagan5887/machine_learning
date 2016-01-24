#!/usr/bin/env python   
#the above just indicates to use python to intepret this file

# ---------------------------------------------------------------
#This mapper code will input a line of text and output <word, 1>
# 
# ---------------------------------------------------------------

import sys             #a python module with system functions for this OS

# Import necessary modules for datetime functionality
from datetime import timedelta
import datetime

date_to_track_from = '2015-12-29'
today_datetime = datetime.date.today()
#today_date = today_datetime.strftime('%Y-%m-%d')
today_date = '2016-01-19'
three_months_delta = timedelta(days=90)
three_months_ago_datetime = today_datetime - three_months_delta
three_months_ago = three_months_ago_datetime.strftime('%Y-%m-%d')
one_year_delta = timedelta(days=365)
one_year_ago_datetime = today_datetime - one_year_delta
one_year_ago = one_year_ago_datetime.strftime('%Y-%m-%d')

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
    data_close_price = data_points[4]
    data_open_price = data_points[1]
    data_high_price = data_points[2]
    data_low_price = data_points[3]
    data_symbol = data_points[7]

    if (data_date != 'Date'):
        flag = None
        if(data_date == today_date):
            flag = 'today'
        elif (data_date == date_to_track_from):
            flag = 'since_crash'
        elif(data_date > three_months_ago):
            flag = 'three_months'
        elif(data_date > one_year_ago):
            flag = 'one_year'

        if not(flag is None):
            print( '{0}_{1}\t{2}\t{3}\t{4}\t{5}\t{6}'.format(data_symbol, data_date, data_close_price, data_open_price, data_high_price, data_low_price, flag) )
