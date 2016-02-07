close_prices_for_span = broadcastSymbolCollectionInstance.value.getSymbolSpanValueVectors('three_month', 'delta_percentage')



print "\n\nClustering for three_month\n\n\n"


close_prices_for_span_values = close_prices_for_span['value_vectors']
close_prices_for_span_symbols = close_prices_for_span['symbols_list']


spanClosePricesRDD = sc.parallelize(close_prices_for_span_values)
spanClosePricesRDD.cache()

spanClosePricesKMModel = KMeans.train(spanClosePricesRDD,k=2,
                           maxIterations=200,runs=3,
                           initializationMode='k-means||',seed=10L)

clusterCenters = spanClosePricesKMModel.clusterCenters

three_month_up_file = open('/tmp/three_month.txt', 'w')


print "\n\nPrinting Cluster Groups\n\n\n"

clusterGroups = ClusterHelper.getKMModelListOfClusterMembersByValueAndKeyVectors(spanClosePricesKMModel, close_prices_for_span_values, close_prices_for_span_symbols)


three_month_up_file.write(str(clusterGroups))


three_month_up_file.close()