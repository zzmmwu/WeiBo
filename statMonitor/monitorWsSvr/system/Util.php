<?php
require_once 'system/log/log.php';

class Util {

    public static function get_rand_str($len){
        $chars = 'abcdefghijklmnopqrstuvwxyz1234567890';
        $str_len = strlen($chars);
        $rand_str = '';
        for($i=0; $i<$len; ++$i){
            $rand_str .= $chars[mt_rand(0, $str_len-1)];
        }
        return $rand_str;
    }
}
