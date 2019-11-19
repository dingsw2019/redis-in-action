<?php

require_once ("../RedisClient.php");

class LogRedisKey {

    /**
     * 记录最近的日志
     * @param string $name 日志名
     * @param string $serverity 日志错误等级
     * @structure list 列表
     * @value 日志内容
     * @return string
     */
    public static function recent($name,$severity){
        return sprintf("recent:%s:%s",$name,$severity);
    }

    /**
     * 记录每小时每条日志出现的数量
     * @param string $name 日志名
     * @param string $serverity 日志错误等级
     * @structure zset 有序集合
     * @member message 消息内容
     * @score count 出现次数
     * @return string
     */
    public static function common($name,$severity){
        return sprintf("common:%s:%s",$name,$severity);
    }

    /**
     * 日志当前所处的小时数
     * @param string $name 日志名
     * @param string $serverity 日志错误等级
     * @structure string 字符串
     * @value 小时数
     * @return string
     */
    public static function common_hour($name,$severity){
        return sprintf("common:%s:%s:start",$name,$severity);
    }
}

class Log {

    //redis-cli
    private $conn;

    public function __construct()
    {
        $this->conn = RedisClient::getConn();
    }

    const LOG_DEBUG = 'debug';
    const LOG_INFO = 'info';
    const LOG_WARNING = 'warning';
    const LOG_ERROR = 'error';
    const LOG_CRITICAL = 'critical';

    const SEVERITY = [

    ];

    /**
     * 记录最近100条日志
     * @param $name
     * @param $message
     * @param string $serverity
     * @param null $pipe
     * @throws Exception
     */
    public function log_recent($name,$message,$serverity=self::LOG_INFO,$pipe=null){

        $serverity = strtolower($serverity);
        //将当前时间添加到消息中,用于记录消息的发送时间
        $message = date('D M d H:i:s Y') . ' ' . $message;
        $pipe = ($pipe) ? $pipe : $this->conn->pipeline();
        //将消息添加到日志列表的最前面
        $pipe->lpush(LogRedisKey::recent($name,$serverity),[$message]);
        //保持列表长度
        $pipe->ltrim(LogRedisKey::recent($name,$serverity),0,99);
        $pipe->execute();
    }

    /**
     * 按分钟存储日志
     * @param $name
     * @param $message
     * @param string $serverity
     * @param int $timeout
     * @throws \Predis\ClientException
     * @throws \Predis\NotSupportedException
     */
    public function log_common($name,$message,$serverity=self::LOG_INFO,$timeout=5){
        $serverity = strtolower($serverity);
        $destination = LogRedisKey::common($name,$serverity);
        //程序每小时切分一次日志,所以它用来记录当前所处小时数
        $start_key = LogRedisKey::common_hour($name,$serverity);
        $trans = $this->conn->transaction(['cas'=>true]);
        $end = microtime(true) + $timeout;

        while (microtime(true) < $end){
            try {
                //监控当前小时数
                $trans->watch($start_key);
                //获取当前所处小时数
                $hour_start = gmdate('Y-m-d\TH:i:00');
                //获取存储的小时数
                $existing = $trans->get($start_key);
                $trans->multi();
                //进入下一个小时
                if ($existing AND $existing < $hour_start) {
                    //旧日志归档
                    $trans->rename($destination,$destination . ":last");
                    $trans->rename($start_key,$destination . ":pstart");
                    //更新所处小时数
                    $trans->set($start_key,$hour_start);
                } else if (!$existing) {
                    $trans->set($start_key,$hour_start);

                }
                //记录相同日志出现次数
                $trans->zincrby($destination,1,$message);
                //记录最近100条日志
                $this->log_recent($name,$message,$serverity,$trans);
                return ;
            }catch (\Predis\Transaction\AbortedMultiExecException $e){
                //pass
            }
        }
    }
}

//$log = new Log();
//添加消息到日志列表
//foreach(range(1,125) as $id){
//    $log->log_recent("request","home page {$id}");
//}

//按分钟存储日志
//foreach(range(1,7) as $id){
//    echo "时间: " . gmdate('Y-m-d\TH:i:00') . PHP_EOL;
//    $log->log_common("request","home page {$id}");
//    if($id != 7){
//        sleep(10);
//    }
//}
