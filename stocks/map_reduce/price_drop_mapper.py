#!/usr/bin/env python   
#the above just indicates to use python to intepret this file

# ---------------------------------------------------------------
#This mapper code will input a line of text and output <word, 1>
# 
# ---------------------------------------------------------------

import sys             #a python module with system functions for this OS

# ------------------------------------------------------------
#  this 'for loop' will set 'line' to an input line from system 
#    standard input file
# ------------------------------------------------------------
for line in sys.stdin:  

#-----------------------------------
#sys.stdin call 'sys' to read a line from standard input, 
# note that 'line' is a string object, ie variable, and it has methods that you can apply to it,
# as in the next line
# ---------------------------------
    line = line.strip()  #strip is a method, ie function, associated
                         #  with string variable, it will strip 
                         #   the carriage return (by default)
    data_points = line.split(',')  #split line at blanks (by default), 
                         #   and return a list of keys
        
    # If this is a header row, ignore it
    data_date = data_points[0]
    data_price = data_points[4]
    data_symbol = data_points[7]
        
    if (data_date != 'Date'):
    
        date_to_track_from = '2015-12-29'
        today_date = '2016-01-19'
    
        if (data_date == date_to_track_from):
            print('{0}\t{1}\t{2}'.format(data_symbol, data_price, 'since') )
        elif(data_date == today_date):
            print('{0}\t{1}\t{2}'.format(data_symbol, data_price, 'today') )
        
    
