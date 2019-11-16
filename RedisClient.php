<?php
/**
 * Created by PhpStorm.
 * User: sbird
 * Date: 2019/10/27
 * Time: 13:00
 */

require __DIR__.'/predis-1.1/autoload.php';

class RedisClient{

    const REDIS_HOST = '127.0.0.1';
    const REDIS_PORT = 6379;
    const REDIS_DATABASE = 15;

    private static $instance;

    public static function getConn(){

        if(self::$instance){
            return self::$instance;
        }
        self::$instance = new Predis\Client([
            'host' => self::REDIS_HOST,
            'port' => self::REDIS_PORT,
//            'database' => self::REDIS_DATABASE,
        ]);

        return self::$instance;
    }

    public static function config_get(){

    }

    public static function config_set(){

    }
}
