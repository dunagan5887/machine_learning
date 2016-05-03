# Machine Learning Code Sample and Demo

## Purpose

The purpose of this repository is to contain a code sample detailing my exploratory work with the Spark framework. As
I have recently become very interested with trading in the stock market, I chose to base my code sample around work
with stock market data. For this, I have produced two code sample:

1. A script which clusters stocks based on their performance over the past year
2. A script which computes which stocks experienced the largest drop in price compared to their largest price drop over
    the past year in the most recent trading session
    
## Stock Clustering

The theory behind this project was that in order to project which stocks would perform well in 2016, we would look at
which stocks performed well over the course of 2015 (call this stock group A). We would take the stocks which performed 
best and most consistently in 2015 and then evaluate their performance in 2014. Whatever stocks in 2015 performed 
similarly to how the stocks in group A performed in 2014 could be expected to perform well in 

This code sample accomplishes the task of clustering stocks by their performance over an arbitrary period of time. This
code sample has hard coded 26 periods of 14 days each, essentially spanning the previous year