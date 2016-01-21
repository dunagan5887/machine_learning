<?php
/**
 * Created by PhpStorm.
 * User: cloudera
 * Date: 1/19/16
 * Time: 3:04 PM
 */

function get_price_delta_since_date($since_date, $symbol)
{
    $file_path = get_data_subdirectory('historical_data') . '/' . $symbol . '.csv';
    $stock_data = file_get_contents($file_path);
    $stock_data_as_array = explode("\n", $stock_data);
    $latest_date_data = $stock_data_as_array[1];

    $latest_stock_day_data = explode(',', $latest_date_data);

    $latest_price = $latest_stock_day_data[4];
    $since_price = 0;
    foreach($stock_data_as_array as $stock_day_data_as_string)
    {
        $stock_day_data = explode(',', $stock_day_data_as_string);
        $date = $stock_day_data[0];
        if ($date == $since_date)
        {
            $since_price = $stock_day_data[4];
            break;
        }
    }

    return floatval($latest_price) - floatval($since_price);
}

$date_starting_crash = '2015-12-29';
$symbols = array('ATEN', 'AHC', 'DDD', 'MMM', 'WBAI','WUBA','ZYNE');

foreach ($symbols as $symbol)
{
    $delta = get_price_delta_since_date($date_starting_crash, $symbol);
    echo $symbol . " " . $delta . "\n";
}

