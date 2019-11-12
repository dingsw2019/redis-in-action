<?php

require_once ("../RedisClient.php");
require_once ("../ch8/social.php");

class QueueRedisKey{

    /**
     * 延迟队列
     * @structure zset 有序集合
     * @member json格式数据,包含:
     * @scores 执行时间戳
     * @return string
     */
    public static function delay(){
        return "delayed:";
    }

    /**
     * 立即执行队列
     * @param string $queue_name 队列名
     * @structure list 列表
     * @value json格式数据
     * @return string
     */
    public static function queue($queue_name){
        return sprintf("queue:%s",$queue_name);
    }
}

class Queue
{
    //redis-cli
    private $conn;

    public function __construct()
    {
        $this->conn = RedisClient::getConn();
    }

    public function acquire_lock($identifier){
        $value = uniqid();
        $result = $this->conn->setnx($identifier,$value);
        if($result){
            return $value;
        }
        return false;
    }

    //
    public function release_lock($identifier,$value){
        $lock_value = $this->conn->get($identifier);
        if($lock_value == $value){
            $this->conn->del($identifier);
            return true;
        }
        return false;
    }

    //加入延迟队列
    public function execute_later($queue, $name, $args, $delay = 0)
    {
        $identifier = uniqid();
        $item = json_encode([$identifier,$queue,$name,$args]);
        if($delay >0){
            $time = time() + $delay;
            $this->conn->zadd(QueueRedisKey::delay(),[$item=>$time]);
        }else{
            $this->conn->rpush(QueueRedisKey::queue($queue),[$item]);
        }
        return $identifier;
    }

    //处理延迟队列
    public function poll_queue(){

        while(true){
            //从延迟队列获取一条最近时间的数据
            $item = $this->conn->zrange(QueueRedisKey::delay(),0,0,['withscores'=>true]);

            if(!$item){
                echo "无数据" . PHP_EOL;
                sleep(1);
                continue;
            }
            //是否到时间
            $item_time = reset($item);
            $item_data = key($item);
            if(!$item or $item_time>time()){
                echo "未到时间" . PHP_EOL;
                sleep(1);
                continue;
            }

            list($identifier,$queue,$name,$args) = json_decode($item_data,true);
            //申请锁,从延迟队列移到执行队列
            $locked = $this->acquire_lock($identifier);
            if(!$locked){
                echo "申请锁失败" . PHP_EOL;
                continue;
            }
            if($this->conn->zrem(QueueRedisKey::delay(),$item_data)){
                echo "删除成功" . PHP_EOL;
                $this->conn->rpush(QueueRedisKey::queue($queue),[$item_data]);
            }
            $this->release_lock($identifier,$locked);
        }
    }

    //处理执行队列
    public function work_watch_queue($queue,$callbacks){

        while(true){
            //获取任务
            $packed = $this->conn->blpop(QueueRedisKey::queue($queue),1);
            if(!$packed){
                echo "未获取数据" . PHP_EOL;
                continue;
            }
            //处理任务
            list($name,$args) = array_slice(json_decode($packed[1],true),-2);
            if(!method_exists($callbacks,$name)){
                echo "不存在方法" .PHP_EOL;
                continue;
            }
            call_user_func_array([$callbacks,$name],$args);
        }
    }

}









