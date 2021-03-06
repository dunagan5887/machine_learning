# Machine Learning Code Sample and Demo

## Purpose

The purpose of this repository is to contain a code sample detailing my exploratory work with the Spark Machine Learning framework
and the Python programming language. As I have recently become very interested with trading in the stock market, 
I chose to base my code sample around work with stock market data. For this, I have produced two code samples:

1. A script which clusters stocks based on their performance over the past year
2. A script which computes which stocks experienced the largest drop in price compared to their largest price drop over
    the past year in the most recent trading session

The primary purpose of this code was to demonstrate familiarity with the Python programming language and the Spark
framework. The purpose was not so much to create an extendable framework for production environments.

## Stock Clustering

The theory behind this project was that in order to project which stocks would perform well in 2016, we would look at
which stocks performed well over the course of 2015 (call this stock group A). We would take the stocks which performed 
best and most consistently in 2015 and then evaluate their performance in 2014. Whatever stocks in 2015 performed 
similarly to how the stocks in group A performed in 2014 could be expected to perform well in 

This code sample accomplishes the task of clustering stocks by their performance over an arbitrary period of time. This
code sample has hard coded 26 periods of 14 days each (referred to as "spans" in the code), essentially spanning the 
previous year. Each stock's "vector" is comprised of 27 data points; 26 data points are the percentage rise/fall over 
each period in the previous year (these percentage rise/falls are referred to as delta_percentages in the code). The 
27th data point is the ratio of the stock's "today" price (the closing price on the day that the script would be run on) 
divided by the average closing price of the stock during the most recent period.
 
Script stocks/scripts/cluster_stocks_by_performance.py is the script which executes the clustering job. It is commented
to provide explanations as to the steps taken in the code. The data is eventually written to a MySQL database for storage
and viewing via a website interface. The methods and functions called in the scripts are also commented and documented.
Directory stocks/python/ contains the modules which are referenced in the cluster_stocks_by_performance.py script. 
Directory stocks/tests/ contains unit tests for a number of the classes used in the cluster_stocks_by_performance.py script

To view the result of the Stock Clustering, the website below has been set up to allow for viewing the result:

- http://demo.developer-magento.com/admin
- User Name: demo_admin
- Password: demo123
- In the menu at the top of the page, navigate to Stocks -> Stock Clusters -> View Stock Cluster Centers. 

This grid shows the clusters, the percentage by which the cluster's price rose/fell (labeled as delta) the number of stocks 
in the cluster and a graph showing the performance of the cluster over the course of the year. The "Delta" and "# of Symbols" 
columns can be sorted and filtered using the interface on the grid. Clicking on a cluster's row will take you to a page showing 
all of the stocks in the cluster. This second page will show the stock's symbol and a graph showing the stock's performance over
the course of the year. These graphs are constructed using 26 data points, each one being the percentage rise/fall in the
price of the stock over the course of a 14 day period. Note that due to the fact that the exact dates used to create these
graphs may not be the exact dates used to render the graphs on wsj.com. Given that Yahoo finance uses every date's closing price
to render their graphs, their site may provide a better method of verifying the graphs.

## Stock Price Drop

The theory behind this project was that stocks which experience a large drop in price are likely to rebound the following day,
offering attractive short-term trading possibilities. The code considers all stocks which fell in the most recent day of trading.
It will take the drop in the most recent day of trading and compute the ratio between that drop and the largest single-day drop
the stock has experienced in the past 52 weeks. The result of this script can be viewed at the website below (which is the same
website listed above):

- http://demo.developer-magento.com/admin
- User Name: demo_admin
- Password: demo123
- In the menu at the top of the page, navigate to Stocks -> Down Stocks -> View Down Stocks

The columns in the grid mean the following:
- Symbol: Symbol representing the stock
- Span Unit Delta % Ratio: If today_unit_delta_percentage is the percentage drop in the stock's price today, and 
 min_span_unit_delta_percentage is the largest percentage drop the stock experienced in the past 52 weeks, this ratio is
 today_unit_delta_percentage / min_span_unit_delta_percentage
- Today Price: The most recent closing price of the stock
- Today Unit Delta %: The percentage by which the stock's price fell in the most recent day of trading

Method StockRdd.getDownStocksDataTuple() is responsible for producing data values "Span Unit Delta % Ratio" and 
 "Today Unit Delta %". This script and its associated code is not as well documented as the Stock Clustering project as
 this was more of an ancillary project.
