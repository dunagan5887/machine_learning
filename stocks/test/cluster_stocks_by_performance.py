
from pyspark import SparkContext
from pyspark.sql import HiveContext


sc = SparkContext("spark://10.211.55.4:7077", "Stock Clustering", pyFiles=['/var/machine_learning/stocks/python/stocks_python.zip'])
sqlContext = HiveContext(sc)

mysql_url = "jdbc:mysql://10.211.55.4:3306/stocks?user=parallels&password=dellc123"

from pyspark.sql import Row
from stockRdd import StockRdd
from dateInterval import DateIntervalManager
from pyspark.mllib.clustering import KMeans
from clusterHelper import ClusterHelper
from rdd_utility import RddUtility
from dunagan_utility import DunaganListUtility

#sample_data_rdd = sc.textFile("/var/data/stocks/sample_data/*.csv").distinct()
#sample_data_rdd = sc.textFile("file:///var/data/stocks/historical_data/*.csv").distinct()
sample_data_rdd = sc.textFile("file:///var/data/stocks/historical_data/*.csv").distinct()
#sample_data_rdd = sc.textFile("file:///var/data/stocks/historical_data/Z*.csv").distinct()

#sample_data_rdd = sc.textFile("/var/data/stocks/historical_data/UIHC.csv").distinct()

#sample_data_rdd = sc.textFile("hdfs://10.0.0.8:9000/stock_data/*.csv").distinct()

today_date = '2016-03-24'
dateDictionaryToCalculateFor = DateIntervalManager.createDateIntervalDictionaryForPastYear(today_date)

# We want to ensure that any stocks being calculated existed during the entire period
number_of_days_in_dictionary = dateDictionaryToCalculateFor.getNumberOfDaysInDictionary()

minimum_number_of_days = int((4.0 / 7.0) * float(number_of_days_in_dictionary))

date_dictionary_code = dateDictionaryToCalculateFor.dictionary_code

def getDatabaseTableName(table_suffix):
    table_name = date_dictionary_code + '_' + table_suffix
    underscored_table_name = table_name.replace('-', '_')
    return underscored_table_name



mapStockCsvToKeyValueClosure = StockRdd.getMapStockCsvToKeyValueForDatesInDictionaryClosure(dateDictionaryToCalculateFor)

symbol_creation_function_closure = StockRdd.getSymbolDataInstanceForDateDictionaryDataPointsClosure(dateDictionaryToCalculateFor, today_date)
symbol_cluster_data_closure = StockRdd.getDataToClusterByDateDictionariesClosure(dateDictionaryToCalculateFor)
symbol_has_none_values_closure = StockRdd.getDoesSymbolTupleHaveNoNoneValueClosure()


print "\n\n\n\nAbout to cache symbol_cluster_data_rdd\n\n\n\n"


symbol_cluster_data_rdd = sample_data_rdd.map(mapStockCsvToKeyValueClosure)\
                                           .filter(lambda line: not(line is None))\
                                           .reduceByKey(lambda a,b : a + b)\
                                           .map(lambda tuple : ( tuple[0], StockRdd.sort_and_compute_deltas( list(tuple[1]) ) ) )\
                                           .filter(lambda tuple : len(list(tuple[1])) > minimum_number_of_days)\
                                           .map(symbol_creation_function_closure)\
                                           .map(symbol_cluster_data_closure)\
                                           .filter(symbol_has_none_values_closure)

symbol_cluster_data_rdd.cache()




print "\n\n\n\nJust cached symbol_cluster_data_rdd\n\n\n\n"




symbols_clustering_lists = symbol_cluster_data_rdd.map(lambda symbolListTuple : map(lambda symbol_data_tuple : symbol_data_tuple[1], symbolListTuple[1]))\
                                                  .filter(lambda list : all(not(value is None) for value in list))


# NEED TO FIGURE out the big O() notatation for the KMeans.train() method below in terms of k/maxIterations/runs



print "\n\n\n\nAbout to cluster\n\n\n\n"


stockKMeansClusterModel = KMeans.train(symbols_clustering_lists,k=1000,
                               maxIterations=100,runs=10,
                               initializationMode='k-means||',seed=10L)

print "\n\n\n\nJust clustered\n\n\n\n"


centers = stockKMeansClusterModel.clusterCenters

print "\n\n\n\nAbout to get clusterGroupsDictionaryRdd\n\n\n\n"


clusterGroupsDictionaryRdd = ClusterHelper.getKMModelDictionaryOfClusterMembersByTuplesRDD(stockKMeansClusterModel, symbol_cluster_data_rdd)

get_overall_delta_percentage_closure = StockRdd.getOverallDeltaPercentageForClusterClosure(dateDictionaryToCalculateFor)

print "\n\n\n\nAbout to map(get_overall_delta_percentage_closure, centers)\n\n\n\n"

cluster_center_overall_deltas = map(get_overall_delta_percentage_closure, centers)

converted_center_delta_list = DunaganListUtility.convert_list_to_value_and_index_tuple_list(cluster_center_overall_deltas)

converted_center_delta_list.sort(lambda tuple_1, tuple_2: cmp(tuple_1[1], tuple_2[1]))

# (ClusterId, Delta-Percentage) Row list construction
converted_center_delta_list_rows = map(lambda delta_tuple: Row(cluster_id=int(delta_tuple[0]), delta_percentage=float(delta_tuple[1])), converted_center_delta_list)

#sqlContext.sql("DROP TABLE cluster_center_overall_delta_percentages")

print "\n\n\n\nAbout to sqlContext.createDataFrame(converted_center_delta_list_rows)\n\n\n\n"

schemaCenterDeltas = sqlContext.createDataFrame(converted_center_delta_list_rows)
#schemaCenterDeltas.saveAsTable("cluster_center_overall_delta_percentages")

print "\n\n\n\nAbout to schemaCenterDeltas.write.jdbc(url=mysql_url, table=cluster_center_overall_delta_percentages_table)\n\n\n\n"

cluster_center_overall_delta_percentages_table = getDatabaseTableName('cluster_total_delta_percentages')
schemaCenterDeltas.write.jdbc(url=mysql_url, table=cluster_center_overall_delta_percentages_table, mode="overwrite")

# (ClusterId,  Smybol) XRef Row List construction

cluster_id_symbol_xref_rows_list = []

for cluster_id, list_of_symbols in clusterGroupsDictionaryRdd.items():
    for symbol in list_of_symbols:
        print "cluster_id: " + str(cluster_id) + "\t\tsymbol: " + symbol
        xrefRow = Row(cluster_id=int(cluster_id), symbol=str(symbol))
        cluster_id_symbol_xref_rows_list.append(xrefRow)

print cluster_id_symbol_xref_rows_list

cluster_id_symbol_xref_rows_rdd = sc.parallelize(cluster_id_symbol_xref_rows_list)

#clusterGroupsDictionary_file = open('/tmp/xref_cluster_symbol.txt', 'w')
#clusterGroupsDictionary_file.write(str(cluster_id_symbol_xref_rows_rdd.collect()))
#clusterGroupsDictionary_file.close()

#sqlContext.sql("DROP TABLE xref_cluster_symbol")

print "\n\n\n\nAbout to schemaClusterIdSymbolXref = sqlContext.createDataFrame(cluster_id_symbol_xref_rows_rdd)\n\n\n\n"


schemaClusterIdSymbolXref = sqlContext.createDataFrame(cluster_id_symbol_xref_rows_rdd)
#schemaClusterIdSymbolXref.saveAsTable('xref_cluster_symbol')

print "\n\n\n\nAbout to schemaClusterIdSymbolXref.write.jdbc(url=mysql_url, table=xref_cluster_symbol_table_name)\n\n\n\n"

xref_cluster_symbol_table_name = getDatabaseTableName('xref_cluster_symbol')
schemaClusterIdSymbolXref.write.jdbc(url=mysql_url, table=xref_cluster_symbol_table_name, mode="overwrite")


# Cluster Center Row List construction

#get_cluster_centers_with_span_codes_closure = getConvertClusterCentersToClusterIdSpanCodeRowsClosure(dateDictionaryToCalculateFor)
get_cluster_centers_with_span_codes_kwargs_closure = StockRdd.getConvertDataListToSpanCodeLabeledDataRowKwargsClosure(dateDictionaryToCalculateFor)
cluster_center_with_span_codes_kwargs_list = map(get_cluster_centers_with_span_codes_kwargs_closure, centers)

cluster_id = 0
for cluster_center_with_span_codes_kwargs in cluster_center_with_span_codes_kwargs_list:
    cluster_center_with_span_codes_kwargs['cluster_id'] = cluster_id
    cluster_id = cluster_id + 1

cluster_center_with_span_codes_rows_list = map(lambda kwargs: Row(**kwargs), cluster_center_with_span_codes_kwargs_list)


#sqlContext.sql("DROP TABLE cluster_center_delta_percentages")

print "\n\n\n\nAbout to dataFrameClusterCenterWithSpanCodes = sqlContext.createDataFrame(cluster_center_with_span_codes_rows_list)\n\n\n\n"

dataFrameClusterCenterWithSpanCodes = sqlContext.createDataFrame(cluster_center_with_span_codes_rows_list)
#dataFrameClusterCenterWithSpanCodes.saveAsTable('cluster_center_delta_percentages')

print "\n\n\n\nAbout to dataFrameClusterCenterWithSpanCodes.write.jdbc(url=mysql_url, table=cluster_center_delta_percentages_table_name)\n\n\n\n"

cluster_center_delta_percentages_table_name = getDatabaseTableName('cluster_span_delta_percentages')
dataFrameClusterCenterWithSpanCodes.write.jdbc(url=mysql_url, table=cluster_center_delta_percentages_table_name, mode="overwrite")

# Stock Row List construction
symbol_data_rows_list = []

for symbol_data_with_span_codes_tuple in symbol_cluster_data_rdd.collect():
    symbol = symbol_data_with_span_codes_tuple[0]
    span_code_and_data_point_tuples_list = symbol_data_with_span_codes_tuple[1]
    kwargs = {'symbol': symbol}
    for span_code_and_data_point_tuple in span_code_and_data_point_tuples_list:
        span_code = span_code_and_data_point_tuple[0]
        data_point = span_code_and_data_point_tuple[1]
        kwargs[span_code] = data_point
    symbolRow = Row(**kwargs)
    symbol_data_rows_list.append(symbolRow)

#sqlContext.sql("DROP TABLE symbol_data_delta_percentages")

print "\n\n\n\nAbout to dataFrameSymbolData = sqlContext.createDataFrame(symbol_data_rows_list)\n\n\n\n"


dataFrameSymbolData = sqlContext.createDataFrame(symbol_data_rows_list)
#dataFrameSymbolData.saveAsTable('symbol_data_delta_percentages')

print "\n\n\n\nAbout to dataFrameSymbolData.write.jdbc(url=mysql_url, table=symbol_data_delta_percentages_table_name)\n\n\n\n"

symbol_data_delta_percentages_table_name = getDatabaseTableName('symbol_delta_percentages')
dataFrameSymbolData.write.jdbc(url=mysql_url, table=symbol_data_delta_percentages_table_name, mode="overwrite")

# Cluster Center Line Graph Data points List construction

get_center_line_graph_data_point_kwargs_closure = StockRdd.getConvertDataListToLineGraphDataPointKwargsClosure(dateDictionaryToCalculateFor)
cluster_center_line_graph_data_point_kwargs_list = map(get_center_line_graph_data_point_kwargs_closure, centers)

cluster_id = 0
for cluster_center_line_graph_data_point_kwargs in cluster_center_line_graph_data_point_kwargs_list:
    cluster_center_line_graph_data_point_kwargs['cluster_id'] = cluster_id
    cluster_id = cluster_id + 1

cluster_center_line_graph_data_point_rows_list = map(lambda kwargs: Row(**kwargs), cluster_center_line_graph_data_point_kwargs_list)

#sqlContext.sql("DROP TABLE cluster_center_line_graph_points")

print "\n\n\n\nAbout to dataFrameClusterCenterLineGraphPoints = sqlContext.createDataFrame(cluster_center_line_graph_data_point_rows_list)\n\n\n\n"


dataFrameClusterCenterLineGraphPoints = sqlContext.createDataFrame(cluster_center_line_graph_data_point_rows_list)
#dataFrameClusterCenterLineGraphPoints.saveAsTable('cluster_center_line_graph_points')

print "\n\n\n\nAbout to dataFrameClusterCenterLineGraphPoints.write.jdbc(url=mysql_url, table=cluster_center_line_graph_points_table_name)\n\n\n\n"

cluster_center_line_graph_points_table_name = getDatabaseTableName('cluster_line_graph_points')
dataFrameClusterCenterLineGraphPoints.write.jdbc(url=mysql_url, table=cluster_center_line_graph_points_table_name, mode="overwrite")

# Symbol Line Graph Data points List construction

symbol_line_graph_points_list = []

date_interval_codes = dateDictionaryToCalculateFor.getDateIntervalCodes()
number_of_date_interval_codes = len(date_interval_codes)

for symbol_data_with_span_codes_tuple in symbol_cluster_data_rdd.collect():
    symbol = symbol_data_with_span_codes_tuple[0]
    span_code_and_data_point_tuples_list = symbol_data_with_span_codes_tuple[1]
    kwargs = {'symbol': symbol}
    total_delta_percentage = 1.0
    index = 0
    for span_code_and_data_point_tuple in span_code_and_data_point_tuples_list:
        if index < number_of_date_interval_codes:
            span_code = span_code_and_data_point_tuple[0]
            data_point = span_code_and_data_point_tuple[1]
            delta_percentage = float(data_point) # NEED TO FIGURE OUT WHY SOME SYMBOLS HAVE NONE FOR data_point here
            total_delta_percentage = total_delta_percentage * (1.0 + delta_percentage)
            kwargs[span_code] = total_delta_percentage
        index = index + 1
    symbolRow = Row(**kwargs)
    symbol_line_graph_points_list.append(symbolRow)

#sqlContext.sql("DROP TABLE symbol_data_line_graph_points")

print "\n\n\n\nAbout to dataFrameSymbolDataLineGraphPoints = sqlContext.createDataFrame(symbol_line_graph_points_list)\n\n\n\n"


dataFrameSymbolDataLineGraphPoints = sqlContext.createDataFrame(symbol_line_graph_points_list)
#dataFrameSymbolDataLineGraphPoints.saveAsTable('symbol_data_line_graph_points')

print "\n\n\n\nAbout to dataFrameSymbolDataLineGraphPoints.write.jdbc(url=mysql_url, table=symbol_data_line_graph_points_table_name)\n\n\n\n"


symbol_data_line_graph_points_table_name = getDatabaseTableName('symbol_line_graph_points')
dataFrameSymbolDataLineGraphPoints.write.jdbc(url=mysql_url, table=symbol_data_line_graph_points_table_name, mode="overwrite")






'''

test_select = sqlContext.sql("SELECT cluster_id, delta_percentage FROM cluster_center_delta_percentages")

test_values = test_select.map(lambda row: "Percentage: " + str(row.delta_percentage))

for value in test_values.collect():
    print value

rddClusterIdSymbolXrefSelect = sqlContext.sql("SELECT cluster_id,symbol FROM xref_cluster_symbol")

for xref in rddClusterIdSymbolXrefSelect.collect():
    print "cluster_id: {0}\t\tsymbol: {1}".format(xref.cluster_id, xref.symbol)


rddClusterIdSymbolXrefSelect = sqlContext.sql("SELECT * FROM xref_cluster_symbol")

for xref in rddClusterIdSymbolXrefSelect.collect():
    print "cluster_id: {0}\t\tsymbol: {1}".format(xref.cluster_id, xref.symbol)



# NEED TO SEE WHY SOME OF THE LISTS CONTAINED NULL VALUES


clusterGroupsDictionary_file = open('/tmp/stocks_symbols_clustering_lists.txt', 'w')
clusterGroupsDictionary_file.write(str(symbols_clustering_lists.collect()))
clusterGroupsDictionary_file.close()

clusterGroupsDictionary_file = open('/tmp/stocks_clusterGroupCenters.txt', 'w')
clusterGroupsDictionary_file.write(str(centers))
clusterGroupsDictionary_file.close()

clusterGroupsDictionary_file = open('/tmp/stocks_clusterGroupsDictionary.txt', 'w')
clusterGroupsDictionary_file.write(str(clusterGroupsDictionary))
clusterGroupsDictionary_file.close()

clusterGroupsDictionary_file = open('/tmp/stocks_converted_center_delta_list.txt', 'w')
clusterGroupsDictionary_file.write(str(converted_center_delta_list))
clusterGroupsDictionary_file.close()






The following was caused by some list-elements of symbols_clustering_lists have None values



16/02/13 19:24:24 INFO Executor: Finished task 6357.0 in stage 43.0 (TID 114443). 1161 bytes result sent to driver
16/02/13 19:24:24 INFO TaskSetManager: Finished task 6357.0 in stage 43.0 (TID 114443) in 2 ms on localhost (6358/6358)
16/02/13 19:24:24 INFO DAGScheduler: ResultStage 43 (collectAsMap at KMeans.scala:302) finished in 11.418 s
16/02/13 19:24:24 INFO TaskSchedulerImpl: Removed TaskSet 43.0, whose tasks have all completed, from pool
16/02/13 19:24:24 INFO DAGScheduler: Job 13 finished: collectAsMap at KMeans.scala:302, took 25.930120 s
16/02/13 19:24:24 INFO MapPartitionsRDD: Removing RDD 11 from persistence list
16/02/13 19:24:24 INFO BlockManager: Removing RDD 11
Traceback (most recent call last):
  File "<stdin>", line 3, in <module>
  File "/home/parallels/Programs/spark/spark-1.5.2-bin-hadoop2.6/python/pyspark/mllib/clustering.py", line 150, in train
    runs, initializationMode, seed, initializationSteps, epsilon)
  File "/home/parallels/Programs/spark/spark-1.5.2-bin-hadoop2.6/python/pyspark/mllib/common.py", line 130, in callMLlibFunc
    return callJavaFunc(sc, api, *args)
  File "/home/parallels/Programs/spark/spark-1.5.2-bin-hadoop2.6/python/pyspark/mllib/common.py", line 123, in callJavaFunc
    return _java2py(sc, func(*args))
  File "/home/parallels/Programs/spark/spark-1.5.2-bin-hadoop2.6/python/lib/py4j-0.8.2.1-src.zip/py4j/java_gateway.py", line 538, in __call__
  File "/home/parallels/Programs/spark/spark-1.5.2-bin-hadoop2.6/python/pyspark/sql/utils.py", line 42, in deco
    raise IllegalArgumentException(s.split(': ', 1)[1])
pyspark.sql.utils.IllegalArgumentException: requirement failed
>>>

'''
