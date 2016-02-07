
from stockRdd import StockRdd
from dunagan_utility import DunaganListUtility


from pyspark import SparkContext

sc = SparkContext("spark://ubuntu:7077", "Stock Clustering", pyFiles=[])


sample_data_rdd = sc.textFile("/var/machine_learning/stocks/data/mini_test_set/*.csv").distinct()

mapped_data = sample_data_rdd.map(StockRdd.mapStockCsvToKeyValue).filter(lambda line: not(line is None))

print "\n\n\n"

print mapped_data.collect()

print "\n\n\n"

print "\n\n\n"
grouped_data = mapped_data.groupByKey()
print grouped_data


grouped_sorted_data_rdd = mapped_data.groupByKey().map(lambda tuple : ( tuple[0], StockRdd.sort_and_compute_deltas( list(tuple[1]) ) ) )\

grouped_sorted_data = grouped_sorted_data_rdd.collect()
print "\n\n\n"
print grouped_sorted_data
print "\n\n\n"
grouped_sorted_data_rdd.cache()
