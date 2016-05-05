<?php
/**
 * Author: Sean Dunagan (https://github.com/dunagan5887)
 * Created: 5/5/16
 */

include('/var/machine_learning/stocks/scripts/import_stock_data.php');

if (isset($argv[1]))
{
    $stock_symbol = $argv[1];

    download_stock_data_history_by_symbol($stock_symbol);
}
