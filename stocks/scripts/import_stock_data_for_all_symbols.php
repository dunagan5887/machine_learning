<?php
/**
 * Author: Sean Dunagan (https://github.com/dunagan5887)
 * Created: 5/5/16
 */

include('./import_stock_data.php');

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
        $stock_data_as_array = explode(',', $stock_data_as_string);
        $symbol_index_with_quotes = reset($stock_data_as_array);
        $symbol = str_replace('"', '', $symbol_index_with_quotes);
        download_stock_data_history_by_symbol($symbol);

        $symbol_count++;
    }
}
