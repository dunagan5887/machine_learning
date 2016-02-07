#!/usr/bin/env python

import sys
import pickle
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans

from clusterHelper import ClusterHelper

from symbolData import SymbolDataCollection

#sc = SparkContext("local", "Stock Clustering", pyFiles=[])

symbol_collection_span_code = 'crash_plus_12_periods_leading_up'

stock_data_output_directory = '/var/machine_learning/stocks/data/stock_data/'
symbol_collection_file = open(stock_data_output_directory + 'cached_symbol_collection.txt', 'r')
symbolCollectionInstance = pickle.load(symbol_collection_file) # type: SymbolDataCollection
symbol_collection_file.close()




print sys.getsizeof(symbolCollectionInstance)
exit()





close_prices_for_span = symbolCollectionInstance.getSymbolSpanValueVectors(symbol_collection_span_code, 'close_price')
close_prices_for_span_values = close_prices_for_span['value_vectors']
close_prices_for_span_symbols = close_prices_for_span['symbols_list']




print close_prices_for_span

exit()

spanClosePricesRDD = sc.parallelize(close_prices_for_span_values)
spanClosePricesRDD.cache()

spanClosePricesKMModel = KMeans.train(spanClosePricesRDD,k=200,
                           maxIterations=200,runs=10,
                           initializationMode='k-means||',seed=10L)

clusterCenters = spanClosePricesKMModel.clusterCenters

clusterCenters_file = open(stock_data_output_directory + 'cached_cluster_centers.txt', 'w')
pickle.dump(clusterCenters, clusterCenters_file)
clusterCenters_file.close()

clusterGroups = ClusterHelper.getKMModelListOfClusterMembers(spanClosePricesKMModel, close_prices_for_span_values, close_prices_for_span_symbols)

clusterGroups_file = open(stock_data_output_directory + 'cached_stock_cluster_groups.txt', 'w')
pickle.dump(clusterGroups, clusterGroups_file)
clusterGroups_file.close()
