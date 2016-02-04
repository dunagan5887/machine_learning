#!/usr/bin/env python

from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans

from clusterHelper import ClusterHelper

sc = SparkContext("local", "Stock Clustering", pyFiles=[])

a_list = [1.0, 2.0, 3.0]
b_list = [1.4, 2.4, 3.4]
c_list = [1.9, 2.9, 3.9]
d_list = [1.8, 2.8, 3.8]

symbol_data_dict = {'a' : a_list, 'b' : b_list, 'c' : c_list, 'd' : d_list}


symbol_data_lists = [a_list, b_list, c_list, d_list]
cachedSymbolsLists = sc.parallelize(symbol_data_lists)
cachedSymbolsLists.cache()



my_kmmodel = KMeans.train(cachedSymbolsLists,k=2,
               maxIterations=200,runs=10,
               initializationMode='k-means||',seed=10L)

centers = my_kmmodel.clusterCenters

print centers

clusterGroups = ClusterHelper.getKMModelListOfClusterMembers(my_kmmodel, symbol_data_dict)

print clusterGroups

