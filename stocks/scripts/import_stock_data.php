<?php
/**
 * Created by PhpStorm.
 * User: cloudera
 * Date: 1/19/16
 * Time: 1:56 PM
 */

include('/var/machine_learning/stocks/scripts/utility.php');
ensure_data_directory_exists();

function get_file_data_as_array($file_path)
{
    $csv_file_data = file_get_contents($file_path);
    $csv_file_data_as_array = explode("\n", $csv_file_data);
    array_shift($csv_file_data_as_array);
    return $csv_file_data_as_array;
}

function download_stock_data_history_by_symbol($stock_symbol)
{
    $year = date('Y');
    $day = intval(date('d'));
    $month = intval(date('m')) - 1;
    $historical_stock_data_link_format = 'http://real-chart.finance.yahoo.com/table.csv?s=%s&a=%s&b=%s&c=2013&d=%s&e=%s&f=%s&g=d&ignore=.csv';
    $link = sprintf($historical_stock_data_link_format, $stock_symbol, $day, $month, $day, $month, $year);
    $historical_stock_data = file_get_contents($link);

    // Header row of file
    // Date,Open,High,Low,Close,Volume,Adj Close,Symbol
    $symbol_included_historical_stock_data = str_replace("\n", ",$stock_symbol\n", $historical_stock_data);

    $historical_stock_data_directory = get_data_subdirectory('historical_data');
    ensure_directory_exists($historical_stock_data_directory);
    $historical_stock_data_filename = $stock_symbol . '.csv';
    $filepath = $historical_stock_data_directory . $historical_stock_data_filename;
    file_put_contents($filepath, $symbol_included_historical_stock_data);
}
