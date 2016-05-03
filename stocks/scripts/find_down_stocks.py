
from pyspark import SparkContext
from pyspark.sql import HiveContext


sc = SparkContext("spark://10.0.0.3:7077", "Stock Clustering", pyFiles=['/var/machine_learning/stocks/python/stocks_python.zip'])
sqlContext = HiveContext(sc)

mysql_url = "jdbc:mysql://10.0.0.3:3306/stocks?user=parallels&password=dellc123"

from pyspark.sql import Row
from stockRdd import StockRdd
from dateInterval import DateIntervalManager
from pyspark.mllib.clustering import KMeans
from clusterHelper import ClusterHelper
from rdd_utility import RddUtility
from dunagan_utility import DunaganListUtility

sample_data_rdd = sc.textFile("file:///var/data/stocks/historical_data/Z*.csv").distinct()

today_date = '2016-03-24'
dailyDateIntervalDictionaryToCalculateFor = DateIntervalManager.createDailyIntervalDictionaryForPastYear(today_date)

number_of_days_in_dictionary = dailyDateIntervalDictionaryToCalculateFor.getNumberOfDaysInDictionary()

minimum_number_of_days = int((4.0 / 7.0) * float(number_of_days_in_dictionary))

mapStockCsvToKeyValueClosure = StockRdd.getMapStockCsvToKeyValueForDatesInDictionaryClosure(dailyDateIntervalDictionaryToCalculateFor)
symbol_creation_function_closure = StockRdd.getSymbolDataInstanceForDateDictionaryDataPointsClosure(dailyDateIntervalDictionaryToCalculateFor, today_date)

get_down_stocks_data_function_closure = StockRdd.getDownStocksDataListClosure(today_date)

symbol_down_stocks_data_filtered = sample_data_rdd.map(mapStockCsvToKeyValueClosure)\
                                           .filter(lambda line: not(line is None))\
                                           .reduceByKey(lambda a,b : a + b)\
                                           .map(lambda tuple : ( tuple[0], StockRdd.sort_and_compute_deltas( list(tuple[1]) ) ) )\
                                           .filter(lambda tuple : len(list(tuple[1])) > minimum_number_of_days)\
                                           .map(symbol_creation_function_closure)\
                                           .filter(lambda symbol_and_instance_tuple: not(symbol_and_instance_tuple[1].getTodayPrice() is None))\
                                           .map(get_down_stocks_data_function_closure)\
                                           .filter(lambda data_tuple: not(data_tuple[1] is None))\
                                           .filter(lambda data_tuple: not(data_tuple[1] == float("inf")))

#clusterGroupsDictionary_file = open('/tmp/symbol_down_stocks_data_non_filtered.txt', 'w')
#clusterGroupsDictionary_file.write(str(symbol_down_stocks_data_filtered.collect()))
#clusterGroupsDictionary_file.close()

symbol_down_stocks_data_filtered_rows = symbol_down_stocks_data_filtered\
                                            .map(lambda tuple : Row(symbol = tuple[0], span_unit_delta_percentage_ratio = tuple[1], today_price = tuple[2], today_unit_delta_percentage = tuple[3]))


schemaDownStocks = sqlContext.createDataFrame(symbol_down_stocks_data_filtered_rows)
#down_stocks_table_name = dailyDateIntervalDictionaryToCalculateFor.getDatabaseTableName('down_stocks')
down_stocks_table_name='down_stocks'
schemaDownStocks.write.jdbc(url=mysql_url, table=down_stocks_table_name, mode="overwrite")


