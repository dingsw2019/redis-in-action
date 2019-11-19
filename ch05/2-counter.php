<?php

require_once ("../RedisClient.php");

class CounterRedisKey {

    public static function known(){
        return 'known:';
    }

    public static function count($prec,$name){
        return sprintf("count:%s:%s",$prec,$name);
    }
}

class Counter {

    //秒为单位的计数器精度,1秒,5秒,1分钟,5分钟,1小时,5小时,1填
    const PRECISION = [1,5,60,300,3600,18000,86400];
    const QUIT = false;
    const SAMPLE_COUNT = 100;
    //redis-cli
    private $conn;

    public function __construct()
    {
        $this->conn = RedisClient::getConn();
    }

    /**
     * 更新计数器
     * @param $name
     * @param int $count
     * @param null $now
     * @throws Exception
     */
    public function update_counter($name,$count=1,$now=null){

        $pipe = $this->conn->pipeline();
        $now = $now ?: microtime(true);
        //为每种精度创建一个计数器
        foreach(self::PRECISION as $prec){
            //当前时间片
            $pnow = intval($now / $prec) * $prec;
            $hash = "{$prec}:{$name}";
            //记录精度计数器
            $pipe->zadd(CounterRedisKey::known(),[$hash=>0]);
            //更新精度计数器
            $pipe->hincrby(CounterRedisKey::count($prec,$name),$pnow,$count);
        }
        $pipe->execute();
    }

    /**
     * 获取精度计数器
     * @param $name
     * @param $precision
     * @return array
     */
    public function get_counter($name,$precision){

        //取出计数器中的精度数据
        $data = $this->conn->hgetall(CounterRedisKey::count($precision,$name));

        //排序
        $to_return = [];
        foreach($data as $k => $v){
            $to_return[] = [intval($k),intval($v)];
        }
        sort($to_return);
        return $to_return;
    }

    /**
     * 删除计数器
     * @throws \Predis\ClientException
     * @throws \Predis\CommunicationException
     * @throws \Predis\NotSupportedException
     * @throws \Predis\Response\ServerException
     */
    public function clean_counters(){
        $trans = $this->conn->transaction(['cas'=>true]);
        $passes = 0;
        while (!self::QUIT) {
            //记录清理操作开始执行的时间
            $start = microtime(true);
            $index = 0;

            while ($index < $this->conn->zcard(CounterRedisKey::known())) {
                //获取被检查计数器的数据
                $hash = $this->conn->zrange(CounterRedisKey::known(),$index,$index);
                $index += 1;
                if(!$hash){
                    break;
                }

                //取得计数器的精度
                $hash = $hash[0];
                $prec = intval(explode(':',$hash)[0]);
                //因为清理程序每60秒循环一次
                //所以需根据计数器的更新频率来判断是否真的有必要对计数器进行清理
                $bprec = intval(floor($prec/60)) ?: 1;
                //如果这个计数器在这次循环不需要进行清理,跳过这个计数器
                if ($passes % $bprec){
                    continue;
                }

                $hkey = sprintf('count:%s',$hash);
                //计算出需要保留什么时间之前的样本
                $cutoff = microtime(true) - self::SAMPLE_COUNT * $prec;
                //获取样本的开始时间
                $samples = array_map('intval',$this->conn->hkeys($hkey));
                sort($samples);
//                $remove = bisect_right()
                $remove = 10;
                if($remove){
                    $this->conn->hdel($hkey,array_slice($samples,0,$remove));
                    if($remove == count($samples)){
                        try{
                            $trans->watch($hkey);
                            if(!$trans->hlen($hkey)){
                                $trans->multi();
                                $trans->zrem(CounterRedisKey::known(),$hash);
                                $trans->execute();
                                $index = -1;
                            }else{
                                $trans->unwatch();
                            }
                        }catch (\Predis\Transaction\AbortedMultiExecException $e){
                            //pass
                        }
                    }
                }
            }


            $passes += 1;
            $duration = min(intval(microtime(true) - $start) + 1, 60);
            sleep(max((60 - $duration),1));
        }
    }
}
