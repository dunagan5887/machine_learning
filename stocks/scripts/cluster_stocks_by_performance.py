# Include all necessary Python libraries and packages
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import Row
from pyspark.mllib.clustering import KMeans

# Import custom-built modules for this script
from stockRdd import StockRdd
from dateInterval import DateIntervalManager
from clusterHelper import ClusterHelper
from dunagan_utility import DunaganListUtility

# In a production-environment example, these values would be configured outside of this file, not within this script
spark_url = "spark://10.211.55.4:7077"
spark_context_name = "Stock Clustering"
included_python_files_package = ['/var/machine_learning/stocks/python/stocks_python.zip']
mysql_url = "jdbc:mysql://localhost:3306/stocks?user=parallels&password=dellc123"
data_files = "file:///var/data/stocks/historical_data/Z*.csv"

# In a production-environment example, this value would be dynamically generated using
today_date = '2016-03-24'

# Instantiate the Spark Context to be used for this script
sc = SparkContext(spark_url, spark_context_name, pyFiles=included_python_files_package)
sqlContext = HiveContext(sc)

# Initialize the RDD with the stock data files
sample_data_rdd = sc.textFile(data_files).distinct()

# Create a dictionary containing date-intervals to represent 26 intervals of 2-week spans spanning the past year
dateDictionaryToCalculateFor = DateIntervalManager.createDateIntervalDictionaryForPastYear(today_date)

# We want to ensure that any stocks being calculated existed during the entire period
number_of_days_in_dictionary = dateDictionaryToCalculateFor.getNumberOfDaysInDictionary()
minimum_number_of_days_for_stock = int((4.0 / 7.0) * float(number_of_days_in_dictionary))

# map_stock_csv_to_key_value_closure is a function closure that will filter out lines of data whose dates are outside of the
#   time-frame we are concerned about
map_stock_csv_to_key_value_closure = StockRdd.getMapStockCsvToKeyValueForDatesInDictionaryClosure(dateDictionaryToCalculateFor)

# symbol_creation_function_closure is a function closure which will convert the list of csv data lines to a SymbolData object
#   which can return the data points we need to cluster a stock
symbol_creation_function_closure = StockRdd.getSymbolDataInstanceForDateDictionaryDataPointsClosure(dateDictionaryToCalculateFor, today_date)

# symbol_cluster_data_closure is a function closure which will convert a SymbolData object to a list of data points which
#   should be used to cluster stocks by
symbol_cluster_data_closure = StockRdd.getDataToClusterByDateDictionariesClosure(dateDictionaryToCalculateFor)

# symbol_has_none_values_closure is a function closure which will return False is any of the values in the list of data points
#   returned by symbol_cluster_data_closure contains a None value; it will return True otherwise
symbol_has_none_values_closure = StockRdd.getDoesSymbolTupleHaveNoNoneValueClosure()

print "\n\n\n\nAbout to Map and Reduce Data\n\n\n\n"

"""
symbol_cluster_data_rdd will be an RDD of tuples. Each tuple will contain a stock symbol in index 0 and a list of tuples
    in index 1. This list of tuples will be of the form (data_points_description, data_point_value). These tuple values
    will comprise the data points by which we will cluster stocks. The various functions necessary to construct
    symbol_cluster_data_rdd are described below:

.map(map_stock_csv_to_key_value_closure) - Take the raw csv input data lines and filter out unnecessary data
.filter(lambda line: not(line is None)) - Filter out all lines which are empty
.reduceByKey(lambda a,b : a + b) - Reduce the filtered data by key. The keys are the stocks' symbols
.map(lambda tuple : ( tuple[0], StockRdd.sort_and_compute_deltas( list(tuple[1]) ) ) ) - Sort the filtered data by date
                                                                                            and compute the percentage change
                                                                                            in stock price from day to day
.filter(lambda tuple : len(list(tuple[1])) > minimum_number_of_days_for_stock) - Filter out stocks which weren't listed on
                                                                                    their respective exchange for the entire
                                                                                    duration of the time frame
.map(symbol_creation_function_closure) - Convert the lists of data points into SymbolData objects. These SymbolData objects
                                            have logic to compute the change in price over a time span or the average closing
                                            price over a time span in addition to other logic
.map(symbol_cluster_data_closure) - Create a list of data points from the SymbolData objects which will be used as the data
                                        to cluster stocks by
.filter(symbol_has_none_values_closure) - Filter out any stocks which have any None values for any of the data points computed
                                            by step ".map(symbol_cluster_data_closure)"

"""

symbol_cluster_data_rdd = sample_data_rdd.map(map_stock_csv_to_key_value_closure)\
                                           .filter(lambda line: not(line is None))\
                                           .reduceByKey(lambda a,b : a + b)\
                                           .map(lambda tuple : ( tuple[0], StockRdd.sort_and_compute_deltas( list(tuple[1]) ) ) )\
                                           .filter(lambda tuple : len(list(tuple[1])) > minimum_number_of_days_for_stock)\
                                           .map(symbol_creation_function_closure)\
                                           .map(symbol_cluster_data_closure)\
                                           .filter(symbol_has_none_values_closure)

print "\n\n\n\nJust mapped and reduced data\n\n\n\n"


print "\n\n\n\nAbout to produce lists to feed to KMeans.train()\n\n\n\n"

"""

symbols_clustering_lists is the list of stock data that will be fed into the KMeans.train() function to produce our clustering
    model. symbolListTuple is a tuple produced by symbol_cluster_data_closure(). symbol_data_tuple[1] will refer to the
    actual data point value for each data point we are going to be using to cluster stock data. We will also once again
    filter out any stocks which have any None values for any of the data points just to be sure that no None values are
    breaking the clustering algorithm

"""

symbols_clustering_lists = symbol_cluster_data_rdd.map(lambda symbolListTuple : map(lambda symbol_data_tuple : symbol_data_tuple[1], symbolListTuple[1]))\
                                                  .filter(lambda list : all(not(value is None) for value in list))

# NEED TO FIGURE out the big O() notatation for the KMeans.train() method below in terms of k/maxIterations/runs

"""

The Big O notation for the KMeans clustering algorithm is O(n*d*k*i*r):
    n - Number of stocks being clustered (5392 as of my most recent execution)
    d - dimensions included for each stock's "vector" (in this script's implementation: 27)
    k - The number of clusters to reduce to (set below as 1000)
    i - The number of iterations (Maxed at 100 below)
    r - The number of runs to compute for (Set to 10 below)

As such, using the numbers above, this particular KMeans clustering exeuction can be worst-case expeced to be on the
order of O(145,584,000,000).

"""

print "\n\n\n\nAbout to produce clustering model\n\n\n\n"

# Execute KMeans.train() as defined by the pyspark.mllib.clustering module
stockKMeansClusterModel = KMeans.train(symbols_clustering_lists,k=1000,
                               maxIterations=100,runs=10,
                               initializationMode='k-means||',seed=10L)

print "\n\n\n\nJust produced clustering model\n\n\n\n"

# Get the centers produced by the clustering model
centers = stockKMeansClusterModel.clusterCenters

print "\n\n\n\nAbout to get clusterGroupsDictionaryRdd\n\n\n\n"

# Produce a dictionary which contains a mapping of cluster numbers to lists of symbols which belong to those clusters
clusterGroupsDictionaryRdd = ClusterHelper.getKMModelDictionaryOfClusterMembersByTuplesRDD(stockKMeansClusterModel, symbol_cluster_data_rdd)

# Returns a function closure which will compute by what percentage each cluster's price rose or fell over the time span
#   defined by dateDictionaryToCalculateFor
get_overall_delta_percentage_closure = StockRdd.getOverallDeltaPercentageForClusterClosure(dateDictionaryToCalculateFor)

print "\n\n\n\nAbout to map(get_overall_delta_percentage_closure, centers)\n\n\n\n"

# Produce a list detailing by what percentage each cluster's price rose or fell over the time span defined by dateDictionaryToCalculateFor
cluster_center_overall_deltas = map(get_overall_delta_percentage_closure, centers)

# Convert the list to a list of tuples mapping cluster number to the performance percentage
converted_center_delta_list = DunaganListUtility.convert_list_to_value_and_index_tuple_list(cluster_center_overall_deltas)

# Sort the list of tuples by performance (in ascending order)
converted_center_delta_list.sort(lambda tuple_1, tuple_2: cmp(tuple_1[1], tuple_2[1]))

# Convert the list mapping cluster_number to performance percentage into a list of Row() object for insertion into a database table
# (ClusterId, Delta-Percentage) Row list construction
converted_center_delta_list_rows = map(lambda delta_tuple: Row(cluster_id=int(delta_tuple[0]), delta_percentage=float(delta_tuple[1])), converted_center_delta_list)

print "\n\n\n\nAbout to sqlContext.createDataFrame(converted_center_delta_list_rows)\n\n\n\n"

# Create a data frame from the list of Rows
schemaCenterDeltas = sqlContext.createDataFrame(converted_center_delta_list_rows)

print "\n\n\n\nAbout to schemaCenterDeltas.write.jdbc(url=mysql_url, table=cluster_total_delta_percentages)\n\n\n\n"

# Write the data frame to the database in table cluster_total_delta_percentages
schemaCenterDeltas.write.jdbc(url=mysql_url, table='cluster_total_delta_percentages', mode="overwrite")

# Produce a list which maps cluster numbers to symbols to produce an xref database table
# (ClusterId,  Symbol) XRef Row List construction
cluster_id_symbol_xref_rows_list = []
for cluster_id, list_of_symbols in clusterGroupsDictionaryRdd.items():
    for symbol in list_of_symbols:
        print "cluster_id: " + str(cluster_id) + "\t\tsymbol: " + symbol
        xrefRow = Row(cluster_id=int(cluster_id), symbol=str(symbol))
        cluster_id_symbol_xref_rows_list.append(xrefRow)

# Produce an RDD to create a data frame from
cluster_id_symbol_xref_rows_rdd = sc.parallelize(cluster_id_symbol_xref_rows_list)

print "\n\n\n\nAbout to schemaClusterIdSymbolXref = sqlContext.createDataFrame(cluster_id_symbol_xref_rows_rdd)\n\n\n\n"

# Create a data frame for the xref_cluster_symbol_table_name table
schemaClusterIdSymbolXref = sqlContext.createDataFrame(cluster_id_symbol_xref_rows_rdd)

print "\n\n\n\nAbout to schemaClusterIdSymbolXref.write.jdbc(url=mysql_url, table=xref_cluster_symbol)\n\n\n\n"

# Write the xref_cluster_symbol table to the database
schemaClusterIdSymbolXref.write.jdbc(url=mysql_url, table='xref_cluster_symbol', mode="overwrite")

# Cluster Center Row List construction
# Get a functionc closure which will convert a list of a cluster's data points into a dictionary which can be used as the **kwargs argument
#   to construct a Row object
get_cluster_centers_with_span_codes_kwargs_closure = StockRdd.getConvertDataListToSpanCodeLabeledDataRowKwargsClosure(dateDictionaryToCalculateFor)

# Convert the cluster data points into dictionaries
cluster_center_with_span_codes_kwargs_list = map(get_cluster_centers_with_span_codes_kwargs_closure, centers)

# Add the cluster_id field to each dictionary of cluster data
cluster_id = 0
for cluster_center_with_span_codes_kwargs in cluster_center_with_span_codes_kwargs_list:
    cluster_center_with_span_codes_kwargs['cluster_id'] = cluster_id
    cluster_id = cluster_id + 1

# Use the cluster data dictionaries to produce Row() objects
cluster_center_with_span_codes_rows_list = map(lambda kwargs: Row(**kwargs), cluster_center_with_span_codes_kwargs_list)

print "\n\n\n\nAbout to dataFrameClusterCenterWithSpanCodes = sqlContext.createDataFrame(cluster_center_with_span_codes_rows_list)\n\n\n\n"

# Create a data frame representing the cluster_span_delta_percentages table
dataFrameClusterCenterWithSpanCodes = sqlContext.createDataFrame(cluster_center_with_span_codes_rows_list)

print "\n\n\n\nAbout to dataFrameClusterCenterWithSpanCodes.write.jdbc(url=mysql_url, table=cluster_span_delta_percentages)\n\n\n\n"

# Create the cluster_span_delta_percentages database table
dataFrameClusterCenterWithSpanCodes.write.jdbc(url=mysql_url, table='cluster_span_delta_percentages', mode="overwrite")

# Below we construct a list of Row objects. Each Row object will contain the data points which comprise the data which
#       was passed to the clustering training function KMeans.train() to create the clustering model
# Initialize the list which will hold the Row objects
symbol_data_rows_list = []
for symbol_data_with_span_codes_tuple in symbol_cluster_data_rdd.collect():
    symbol = symbol_data_with_span_codes_tuple[0]
    span_code_and_data_point_tuples_list = symbol_data_with_span_codes_tuple[1]
    # Initialize the kwargs dictionary which will be used to create the Row object for this symbol
    kwargs = {'symbol': symbol}
    for span_code_and_data_point_tuple in span_code_and_data_point_tuples_list:
        # Use the span_code as the name of the database column
        span_code = span_code_and_data_point_tuple[0]
        data_point = span_code_and_data_point_tuple[1]
        kwargs[span_code] = data_point
    symbolRow = Row(**kwargs)
    symbol_data_rows_list.append(symbolRow)

print "\n\n\n\nAbout to dataFrameSymbolData = sqlContext.createDataFrame(symbol_data_rows_list)\n\n\n\n"

# Create data frame for table symbol_data_delta_percentages_table_name
dataFrameSymbolData = sqlContext.createDataFrame(symbol_data_rows_list)

print "\n\n\n\nAbout to dataFrameSymbolData.write.jdbc(url=mysql_url, table=symbol_delta_percentages)\n\n\n\n"

# Write the symbol_delta_percentages database table to the database
dataFrameSymbolData.write.jdbc(url=mysql_url, table='symbol_delta_percentages', mode="overwrite")


# Below we will construct a database table whose rows will represent the points on a line graph which will show
#   each cluster's performance over the course of the time frame represented by dateDictionaryToCalculateFor
# Cluster Center Line Graph Data points List construction

# Get the function closure which will produce kwargs dictionaries for instantiating Row objects
get_center_line_graph_data_point_kwargs_closure = StockRdd.getConvertDataListToLineGraphDataPointKwargsClosure(dateDictionaryToCalculateFor)

# Produce a list of Row objects which represent the cluster centers's performance graph
cluster_center_line_graph_data_point_kwargs_list = map(get_center_line_graph_data_point_kwargs_closure, centers)

# Add the cluster_id to each kwargs dictionary
cluster_id = 0
for cluster_center_line_graph_data_point_kwargs in cluster_center_line_graph_data_point_kwargs_list:
    cluster_center_line_graph_data_point_kwargs['cluster_id'] = cluster_id
    cluster_id = cluster_id + 1

# Produce Row objects from the kwargs dictionaries
cluster_center_line_graph_data_point_rows_list = map(lambda kwargs: Row(**kwargs), cluster_center_line_graph_data_point_kwargs_list)

print "\n\n\n\nAbout to dataFrameClusterCenterLineGraphPoints = sqlContext.createDataFrame(cluster_center_line_graph_data_point_rows_list)\n\n\n\n"

# Create a data frame to hold the data
dataFrameClusterCenterLineGraphPoints = sqlContext.createDataFrame(cluster_center_line_graph_data_point_rows_list)

print "\n\n\n\nAbout to dataFrameClusterCenterLineGraphPoints.write.jdbc(url=mysql_url, table=cluster_line_graph_points)\n\n\n\n"

# Create a database table from the data
dataFrameClusterCenterLineGraphPoints.write.jdbc(url=mysql_url, table='cluster_line_graph_points', mode="overwrite")

# Below we will construct a database table which will hold rows containing line graph points to show each symbol's
#   performance over the time frame defined by dateDictionaryToCalculateFor.
# Symbol Line Graph Data points List construction

symbol_line_graph_points_list = []

date_interval_codes = dateDictionaryToCalculateFor.getDateIntervalCodes()
number_of_date_interval_codes = len(date_interval_codes)

# For each symbol in the symbol_cluster_data_rdd RDD
for symbol_data_with_span_codes_tuple in symbol_cluster_data_rdd.collect():
    symbol = symbol_data_with_span_codes_tuple[0]
    span_code_and_data_point_tuples_list = symbol_data_with_span_codes_tuple[1]
    # Initialize the kwargs dictionary which will be used to construct the Row object
    kwargs = {'symbol': symbol}
    total_delta_percentage = 1.0
    index = 0
    for span_code_and_data_point_tuple in span_code_and_data_point_tuples_list:
        # The first `number_of_date_interval_codes` values in the list represent the percentage change in each symbol's
        #       price over spans of time
        if index < number_of_date_interval_codes:
            # Use the span_code as the name of the database column
            span_code = span_code_and_data_point_tuple[0]
            data_point = span_code_and_data_point_tuple[1]
            delta_percentage = float(data_point) # NEED TO FIGURE OUT WHY SOME SYMBOLS HAVE NONE FOR data_point here
            # total_delta_percentage represents the ratio in the symbol's price after this time interval relative to
            #       its price at the beginning of the time frame defined by dateDictionaryToCalculateFor
            total_delta_percentage = total_delta_percentage * (1.0 + delta_percentage)
            kwargs[span_code] = total_delta_percentage
        index = index + 1
    symbolRow = Row(**kwargs)
    symbol_line_graph_points_list.append(symbolRow)

print "\n\n\n\nAbout to dataFrameSymbolDataLineGraphPoints = sqlContext.createDataFrame(symbol_line_graph_points_list)\n\n\n\n"

# Create a data frame from the symbol_line_graph_points_list
dataFrameSymbolDataLineGraphPoints = sqlContext.createDataFrame(symbol_line_graph_points_list)

print "\n\n\n\nAbout to dataFrameSymbolDataLineGraphPoints.write.jdbc(url=mysql_url, table=symbol_line_graph_points)\n\n\n\n"

# Write the data frame to the database
dataFrameSymbolDataLineGraphPoints.write.jdbc(url=mysql_url, table='symbol_line_graph_points', mode="overwrite")
