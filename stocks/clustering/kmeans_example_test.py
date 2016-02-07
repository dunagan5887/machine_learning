#A script to execute kmeans clustering in spark
#to run enter: >>> exec(open("./dokmeans.py").read())

import numpy
import numpy as np
from pyspark.mllib.clustering import KMeans
from pyspark import SparkContext

sc = SparkContext("spark://ubuntu:7077", "Stock Clustering", pyFiles=[])

#concatenate 2 RDDs with  .union(other) function
test_list = [1,2,3]
my_data=sc.parallelize(test_list)


my_kmmodel = KMeans.train(my_data,k=1,
               maxIterations=20,runs=1,
               initializationMode='k-means||',seed=10L)


 
