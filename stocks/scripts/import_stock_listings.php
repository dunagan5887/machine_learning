<?php

include('./utility.php');
ensure_data_directory_exists();

$nyse_download_link = "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NYSE&render=download";
$nasdaq_download_link = "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download";

$listings_dir = get_data_subdirectory('listings');
ensure_directory_exists($listings_dir);

$data = file_get_contents($nyse_download_link);
file_put_contents($listings_dir . "nyse.csv", $data);

$data = file_get_contents($nasdaq_download_link);
file_put_contents($listings_dir . "nasdaq.csv", $data);
