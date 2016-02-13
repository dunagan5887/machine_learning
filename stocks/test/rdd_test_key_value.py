

from pyspark import SparkContext

sc = SparkContext("spark://ubuntu:7077", "Stock Clustering", pyFiles=[])


#sample_data_rdd = sc.textFile("/var/machine_learning/stocks/data/sample_data/*.csv").distinct()


from stockRdd import StockRdd
from dateInterval import DateIntervalManager
from pyspark.mllib.clustering import KMeans
from clusterHelper import ClusterHelper
from rdd_utility import RddUtility

sample_data_rdd = sc.textFile("/var/machine_learning/stocks/data/historical_data/*.csv").distinct()

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


symbols_clustering_lists = symbol_cluster_data_rdd.map(lambda tuple : tuple[1])


stockKMeansClusterModel = KMeans.train(symbols_clustering_lists,k=2,
                               maxIterations=200,runs=10,
                               initializationMode='k-means||',seed=10L)




centers = stockKMeansClusterModel.clusterCenters

clusterGroupsDictionary = ClusterHelper.getKMModelDictionaryOfClusterMembersByTuplesRDD(stockKMeansClusterModel, symbol_cluster_data_rdd)

clusterGroupsDictionary_file = open('/tmp/clusterGroupCenters.txt', 'w')
clusterGroupsDictionary_file.write(str(centers))
clusterGroupsDictionary_file.close()

clusterGroupsDictionary_file = open('/tmp/clusterGroupsDictionary.txt', 'w')
clusterGroupsDictionary_file.write(str(clusterGroupsDictionary))
clusterGroupsDictionary_file.close()



myKMModel = stockKMeansClusterModel
rddOfClusterNumberToKey = symbol_cluster_data.map(predictionForTupleClosure)
rddOfClusterNumberToKey.collect()



reducedToListRddOfClusterNumberToKey = RddUtility.reduceKeyValuePairRddToKeyListRdd(rddOfClusterNumberToKey)
reducedToListRddOfClusterNumberToKey.collect()


reducedToListRddOfClusterNumberToKey = reduceKeyValuePairRddToKeyListRdd(rddOfClusterNumberToKey)
reducedToListRddOfClusterNumberToKey.collect()

reduceKeyValueRddToDictionaryTest = reduceKeyValueRddToDictionary(reducedToListRddOfClusterNumberToKey)
reduceKeyValueRddToDictionaryTest