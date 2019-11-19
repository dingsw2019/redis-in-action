<?php

require_once ("../RedisClient.php");

class IpRedisKey {

}

class Ip {

    //redis-cli
    private $conn;

    public function __construct()
    {
        $this->conn = RedisClient::getConn();
    }
}
