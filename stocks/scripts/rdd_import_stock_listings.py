from pyspark import SparkContext
from subprocess import call

# In a production-environment example, these values would be configured outside of this file, not within this script
spark_url = "spark://10.211.55.4:7077"
spark_context_name = "Import Stock Listings"
listing_files = "file:///var/data/stocks/listings/*.csv"

# Instantiate the Spark Context to be used for this script
sc = SparkContext(spark_url, spark_context_name)

sample_data_rdd = sc.textFile(listing_files)

def execute_call_for_symbol(symbol):
    call(["php", "/var/machine_learning/stocks/scripts/import_stock_data_for_symbol.php", symbol])

def get_unescped_first_csv_column(listing_row):
    listing_data_points = listing_row.split(',')
    stock_symbol = listing_data_points[0]
    stock_symbol = stock_symbol.replace('"', '')
    stock_symbol = stock_symbol.replace("\t", '')
    if stock_symbol == 'Symbol':
        return None
    return stock_symbol

sample_data_rdd.map(get_unescped_first_csv_column).filter(lambda symbol: not(symbol is None)).foreach(execute_call_for_symbol)

