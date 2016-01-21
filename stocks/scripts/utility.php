<?php
/**
 * Author: Sean Dunagan (https://github.com/dunagan5887)
 * Created: 1/21/16
 */

function get_data_directory()
{
    return __DIR__ . '/../data';
}

function get_data_subdirectory($sub_directory)
{
    $data_directory = get_data_directory();
    return $data_directory . '/' . $sub_directory . '/';
}

function ensure_directory_exists($directory)
{
    if (!is_dir($directory))
    {
        mkdir($directory);
    }
}

function ensure_data_directory_exists()
{
    $data_directory = get_data_directory();
    ensure_directory_exists($data_directory);
}
