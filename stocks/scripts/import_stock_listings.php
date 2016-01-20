<?php

$nyse_download_link = "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NYSE&render=download";
$nasdaq_download_link = "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download";

$download_dir = "/var/machine_learning/stocks/listings/";

$data = file_get_contents($nyse_download_link);
file_put_contents($download_dir . "nyse.csv", $data);

$data = file_get_contents($nasdaq_download_link);
file_put_contents($download_dir . "nasdaq.csv", $data);
