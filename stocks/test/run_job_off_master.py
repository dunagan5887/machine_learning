
# sc type: SparkContext

symbolCollectionInstance = broadcastSymbolCollectionInstance.value

close_prices_for_span = symbolCollectionInstance.getSymbolSpanValueVectors('crash_plus_12_periods_leading_up', 'delta_percentage')


print "\n\nClustering for crash_plus_12_periods_leading_up\n\n\n"


close_prices_for_span_values = close_prices_for_span['value_vectors']
close_prices_for_span_symbols = close_prices_for_span['symbols_list']


spanClosePricesRDD = sc.parallelize(close_prices_for_span_values)
spanClosePricesRDD.cache()

spanClosePricesKMModel = KMeans.train(spanClosePricesRDD,k=2,
                           maxIterations=200,runs=3,
                           initializationMode='k-means||',seed=10L)

clusterCenters = spanClosePricesKMModel.clusterCenters


crash_plus_12_periods_leading_up_file = open('/tmp/crash_plus_12_periods_leading_up.txt', 'w')


print "\n\nPrinting Cluster Groups\n\n\n"



clusterGroups = ClusterHelper.getKMModelListOfClusterMembersByValueAndKeyVectors(spanClosePricesKMModel, close_prices_for_span_values, close_prices_for_span_symbols)

crash_plus_12_periods_leading_up_file.write(str(clusterGroups))


crash_plus_12_periods_leading_up_file.close()