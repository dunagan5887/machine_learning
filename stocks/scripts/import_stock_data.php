<?php
/**
 * Created by PhpStorm.
 * User: cloudera
 * Date: 1/19/16
 * Time: 1:56 PM
 */

include('./utility.php');
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
    $historical_stock_data_link_format = 'http://real-chart.finance.yahoo.com/table.csv?s=%s&a=03&b=12&c=1900&d=%s&e=%s&f=%s&g=d&ignore=.csv';
    $link = sprintf($historical_stock_data_link_format, $stock_symbol, $day, $month, $year);
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

$listings_dir = get_data_subdirectory('listings');
ensure_directory_exists($listings_dir);
$nyse_file = $listings_dir . "nyse.csv";
$nasdaq_file = $listings_dir . "nasdaq.csv";

$files_array = array($nyse_file, $nasdaq_file);

$symbol_count = 0;

foreach($files_array as $file_path)
{
    $csv_file_data_as_array = get_file_data_as_array($file_path);
    foreach($csv_file_data_as_array as $stock_data_as_string)
    {

        /*
        if ($symbol_count > 5)
        {
            exit;
        }
        */


        $stock_data_as_array = explode(',', $stock_data_as_string);
        $symbol_index_with_quotes = reset($stock_data_as_array);
        $symbol = str_replace('"', '', $symbol_index_with_quotes);
        download_stock_data_history_by_symbol($symbol);

        $symbol_count++;
    }
}

