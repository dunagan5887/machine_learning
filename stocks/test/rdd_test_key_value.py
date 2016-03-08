

from pyspark import SparkContext
from pyspark.sql import HiveContext

sc = SparkContext("spark://ubuntu:7077", "Stock Clustering", pyFiles=[])
sqlContext = HiveContext(sc)

#sample_data_rdd = sc.textFile("/var/machine_learning/stocks/data/sample_data/*.csv").distinct()

sample_data_rdd = sc.textFile("/var/machine_learning/stocks/data/historical_data/*.csv").distinct()


from pyspark.sql import Row
from stockRdd import StockRdd
from dateInterval import DateIntervalManager
from pyspark.mllib.clustering import KMeans
from clusterHelper import ClusterHelper
from rdd_utility import RddUtility
from dunagan_utility import DunaganListUtility

sample_data_rdd = sc.textFile("/var/machine_learning/stocks/data/sample_data/*.csv").distinct()

pastYearDateIntervalDictionary = DateIntervalManager.createDateIntervalDictionaryForPastYear()

past_year_date_code = 'past_year'
today_date = '2016-01-19'
mapStockCsvToKeyValueClosure = StockRdd.getMapStockCsvToKeyValueForDatesInDictionaryClosure(pastYearDateIntervalDictionary)

symbol_creation_function_closure = StockRdd.getSymbolDataInstanceForDateDictionaryDataPointsClosure(pastYearDateIntervalDictionary, today_date)
symbol_cluster_data_closure = StockRdd.getDataToClusterByDateDictionariesClosure(pastYearDateIntervalDictionary)

symbol_cluster_data_rdd = sample_data_rdd.map(mapStockCsvToKeyValueClosure)\
                                           .filter(lambda line: not(line is None))\
                                           .reduceByKey(lambda a,b : a + b)\
                                           .map(lambda tuple : ( tuple[0], StockRdd.sort_and_compute_deltas( list(tuple[1]) ) ) )\
                                           .filter(lambda tuple : len(list(tuple[1])) > 180)\
                                           .map(symbol_creation_function_closure)\
                                           .map(symbol_cluster_data_closure)


symbol_cluster_data_rdd.cache()

symbols_clustering_lists = symbol_cluster_data_rdd.map(lambda symbolListTuple : map(lambda symbol_data_tuple : symbol_data_tuple[1], symbolListTuple[1]))\
                                                  .filter(lambda list : all(not(value is None) for value in list))

stockKMeansClusterModel = KMeans.train(symbols_clustering_lists,k=3,
                               maxIterations=200,runs=10,
                               initializationMode='k-means||',seed=10L)

centers = stockKMeansClusterModel.clusterCenters

clusterGroupsDictionary = ClusterHelper.getKMModelDictionaryOfClusterMembersByTuplesRDD(stockKMeansClusterModel, symbol_cluster_data_rdd)

get_overall_delta_percentage_closure = StockRdd.getOverallDeltaPercentageForClusterClosure(pastYearDateIntervalDictionary)
cluster_center_overall_deltas = map(get_overall_delta_percentage_closure, centers)

converted_center_delta_list = DunaganListUtility.convert_list_to_value_and_index_tuple_list(cluster_center_overall_deltas)

converted_center_delta_list.sort(lambda tuple_1, tuple_2: cmp(tuple_1[1], tuple_2[1]))


# (ClusterId, Delta-Percentage) Row list construction
converted_center_delta_list_rows = map(lambda delta_tuple: Row(cluster_id=int(delta_tuple[0]), delta_percentage=float(delta_tuple[1])), converted_center_delta_list)

sqlContext.sql("DROP TABLE cluster_center_overall_delta_percentages")

schemaCenterDeltas = sqlContext.createDataFrame(converted_center_delta_list_rows)
schemaCenterDeltas.saveAsTable("cluster_center_overall_delta_percentages")

# (ClusterId,  Smybol) XRef Row List construction

cluster_id_symbol_xref_rows_list = []

for cluster_id, list_of_symbols in clusterGroupsDictionary.items():
    for symbol in list_of_symbols:
        print "cluster_id: " + str(cluster_id) + "\t\tsymbol: " + symbol
        xrefRow = Row(cluster_id=int(cluster_id), symbol=str(symbol))
        cluster_id_symbol_xref_rows_list.append(xrefRow)

sqlContext.sql("DROP TABLE xref_cluster_symbol")

schemaClusterIdSymbolXref = sqlContext.createDataFrame(cluster_id_symbol_xref_rows_list)
schemaClusterIdSymbolXref.saveAsTable('xref_cluster_symbol')

# Cluster Center Row List construction

#get_cluster_centers_with_span_codes_closure = getConvertClusterCentersToClusterIdSpanCodeRowsClosure(pastYearDateIntervalDictionary)
get_cluster_centers_with_span_codes_kwargs_closure = StockRdd.getConvertDataListToSpanCodeLabeledDataRowKwargsClosure(pastYearDateIntervalDictionary)
cluster_center_with_span_codes_kwargs_list = map(get_cluster_centers_with_span_codes_kwargs_closure, centers)

cluster_id = 0
for cluster_center_with_span_codes_kwargs in cluster_center_with_span_codes_kwargs_list:
    cluster_center_with_span_codes_kwargs['cluster_id'] = cluster_id
    cluster_id = cluster_id + 1

cluster_center_with_span_codes_rows_list = map(lambda kwargs: Row(**kwargs), cluster_center_with_span_codes_kwargs_list)

sqlContext.sql("DROP TABLE cluster_center_delta_percentages")

dataFrameClusterCenterWithSpanCodes = sqlContext.createDataFrame(cluster_center_with_span_codes_rows_list)
dataFrameClusterCenterWithSpanCodes.saveAsTable('cluster_center_delta_percentages')


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

sqlContext.sql("DROP TABLE symbol_data_delta_percentages")

dataFrameSymbolData = sqlContext.createDataFrame(symbol_data_rows_list)
dataFrameSymbolData.saveAsTable('symbol_data_delta_percentages')

# Cluster Center Line Graph Data points List construction

get_center_line_graph_data_point_kwargs_closure = StockRdd.getConvertDataListToLineGraphDataPointKwargsClosure(pastYearDateIntervalDictionary)
cluster_center_line_graph_data_point_kwargs_list = map(get_center_line_graph_data_point_kwargs_closure, centers)

cluster_id = 0
for cluster_center_line_graph_data_point_kwargs in cluster_center_line_graph_data_point_kwargs_list:
    cluster_center_line_graph_data_point_kwargs['cluster_id'] = cluster_id
    cluster_id = cluster_id + 1

cluster_center_line_graph_data_point_rows_list = map(lambda kwargs: Row(**kwargs), cluster_center_line_graph_data_point_kwargs_list)

sqlContext.sql("DROP TABLE cluster_center_line_graph_points")

dataFrameClusterCenterLineGraphPoints = sqlContext.createDataFrame(cluster_center_line_graph_data_point_rows_list)
dataFrameClusterCenterLineGraphPoints.saveAsTable('cluster_center_line_graph_points')


# Symbol Line Graph Data points List construction

symbol_line_graph_points_list = []

date_interval_codes = pastYearDateIntervalDictionary.getDateIntervalCodes()
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
            delta_percentage = float(data_point)
            total_delta_percentage = total_delta_percentage * (1.0 + delta_percentage)
            kwargs[span_code] = total_delta_percentage
        index = index + 1
    symbolRow = Row(**kwargs)
    symbol_line_graph_points_list.append(symbolRow)

sqlContext.sql("DROP TABLE symbol_data_line_graph_points")

dataFrameSymbolDataLineGraphPoints = sqlContext.createDataFrame(symbol_line_graph_points_list)
dataFrameSymbolDataLineGraphPoints.saveAsTable('symbol_data_line_graph_points')









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





'''
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
