<?php
/**
 * Created by PhpStorm.
 * User: ayisun
 * Date: 2016/10/12
 * Time: 11:38
 */

class load_config {
    /**
     * @param $file_path
     * @return array
     */
    public function fc_load_config($file_path){

        $config = parse_ini_file($file_path);
        return $config;
    }
}