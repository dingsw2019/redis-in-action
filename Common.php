<?php
/**
 * 通用类
 */


require_once ("RedisClient.php");

class Common {

    //redis key的默认过期时间
    const DEFAULT_TTL = 300;

    //redis pipeline是否立即执行
    const EXECUTE_TRUE = true;
    const EXECUTE_FALSE = false;

    //常见停用词
    const STOP_WORDS = " tis, twas, a, able, about, across, after, ain't, all, almost, also, am, among, an, and, any, are, aren't, as, at, be, because, been, but, by, can, can't, cannot, could, could've, couldn't, dear, did, didn't, do, does, doesn't, don't, either, else, ever, every, for, from, get, got, had, has, hasn't, have, he, he'd, he'll, he's, her, hers, him, his, how, how'd, how'll, how's, however, i, i'd, i'll, i'm, i've, if, in, into, is, isn't, it, it's, its, just, least, let, like, likely, may, me, might, might've, mightn't, most, must, must've, mustn't, my, neither, no, nor, not, of, off, often, on, only, or, other, our, own, rather, said, say, says, shan't, she, she'd, she'll, she's, should, should've, shouldn't, since, so, some, than, that, that'll, that's, the, their, them, then, there, there's, these, they, they'd, they'll, they're, they've, this, tis, to, too, twas, us, wants, was, wasn't, we, we'd, we'll, we're, were, weren't, what, what'd, what's, when, when, when'd, when'll, when's, where, where'd, where'll, where's, which, while, who, who'd, who'll, who's, whom, why, why'd, why'll, why's, will, with, won't, would, would've, wouldn't, yet, you, you'd, you'll, you're, you've, your";
    //停用词筛选的最小长度
    const FILTER_STOP_WORD_LENGTH = 2;
    //运行模式
    const MODE_REMARK = "remark";
    const MODE_PURE = "pure";
    //关键词缓存
    private $cache_words = [];
    //运行模式,是否返回注释
    private $run_mode;
    private $conn;

    public function __construct(string $mode)
    {
        $this->conn = RedisClient::getConn();
        $this->run_mode = ($mode==self::MODE_REMARK) ? $mode : self::MODE_PURE;
    }

    private static function store_key($id){
        return sprintf("idx:%s",$id);
    }

    private static function common_submit($conn,$method,$keys,$options=[],$ttl=self::DEFAULT_TTL,$execute=self::EXECUTE_TRUE){

        $pipe = ($execute) ? $conn->pipeline() : $conn;
        //调用各种store
        $storeKey = self::store_key(uniqid());
        if($options){
            call_user_func([$pipe,$method],$storeKey,$keys,$options);
        }else{
            call_user_func([$pipe,$method],$storeKey,$keys);
        }
        //设置过期时间
        $pipe->expire($storeKey,$ttl);
        //是否立即执行
        if($execute){
            $pipe->execute();
        }
        return $storeKey;
    }

    private static function common_submit_with_remark($conn,$method,$keys,$options=[],$ttl=self::DEFAULT_TTL,$execute=self::EXECUTE_TRUE){

        $pipe = ($execute) ? $conn->pipeline() : $conn;
        //调用各种store
        $storeKey = self::store_key(uniqid());
        //redis命令,用于命令可视化
        $numKey = (strpos($method,'z')===0) ? count($keys)." " : "";
        $command = "{$method} {$storeKey} {$numKey}" . implode(" ",$keys);
        if($options){
            call_user_func([$pipe,$method],$storeKey,$keys,$options);
            //option转换成redis命令的格式
            $command .= " ". implode(" ",array_map(function($option,$value){
                    if(is_array($value)){
                        $value = implode(" ",$value);
                    }
                    return "{$option} {$value}";
                },array_keys($options),$options));
        }else{
            call_user_func([$pipe,$method],$storeKey,$keys);
        }
        echo $command;
        //设置过期时间
        $pipe->expire($storeKey,$ttl);
        //是否立即执行
        if($execute){
            list($store,$expire) = $pipe->execute();
            if($store && $expire){
                echo " [执行成功]" . PHP_EOL;
                return $storeKey;
            }
            echo " [执行失败]".PHP_EOL;
        }
        echo " [延后执行]" . PHP_EOL;
        return $storeKey;
    }

    public function sinterstore($conn,$keys,$ttl=self::DEFAULT_TTL,$execute=self::EXECUTE_TRUE){
        if($this->run_mode == self::MODE_REMARK){
            return self::common_submit_with_remark($conn,__FUNCTION__,$keys,[],$ttl,$execute);
        }else{
            return self::common_submit($conn,__FUNCTION__,$keys,[],$ttl,$execute);
        }
    }
    public function sunionstore($conn,$keys,$ttl=self::DEFAULT_TTL,$execute=self::EXECUTE_TRUE){
        if($this->run_mode == self::MODE_REMARK){
            return self::common_submit_with_remark($conn,__FUNCTION__,$keys,[],$ttl,$execute);
        }else{
            return self::common_submit($conn,__FUNCTION__,$keys,[],$ttl,$execute);
        }
    }
    public function sdiffstore($conn,$keys,$ttl=self::DEFAULT_TTL,$execute=self::EXECUTE_TRUE){
        if($this->run_mode == self::MODE_REMARK){
            return self::common_submit_with_remark($conn,__FUNCTION__,$keys,[],$ttl,$execute);
        }else{
            return self::common_submit($conn,__FUNCTION__,$keys,[],$ttl,$execute);
        }
    }
    public function zinterstore($conn,$keys,$options=[],$ttl=self::DEFAULT_TTL,$execute=self::EXECUTE_TRUE){
        if($this->run_mode == self::MODE_REMARK){
            return self::common_submit_with_remark($conn,__FUNCTION__,$keys,$options,$ttl,$execute);
        }else{
            return self::common_submit($conn,__FUNCTION__,$keys,$options,$ttl,$execute);
        }
    }
    public function zunionstore($conn,$keys,$options=[],$ttl=self::DEFAULT_TTL,$execute=self::EXECUTE_TRUE){
        if($this->run_mode == self::MODE_REMARK){
            return self::common_submit_with_remark($conn,__FUNCTION__,$keys,$options,$ttl,$execute);
        }else{
            return self::common_submit($conn,__FUNCTION__,$keys,$options,$ttl,$execute);
        }
    }

    /**
     * 提取关键词
     * @param string $content 描述
     * @param int $len 可提取字符的最小长度
     * @return array
     */
    private function content_to_word(string $content,string $delimiter = ',',$len = self::FILTER_STOP_WORD_LENGTH){

        $id = md5($content);
        //提取关键词
        if(!isset($this->cache_words[$id])){
            $words = [];
            if($delimiter !== ' '){
                $content = str_replace(' ','',strtolower($content));
            }
            if($content){
                foreach(explode($delimiter,$content) as $word){
                    if(strlen($word)>=$len && !isset($words[$word])){
                        $words[$word] = $word;
                    }
                }
                $words = array_values($words);
            }
            $this->cache_words[$id] = $words;
        }

        return $this->cache_words[$id];
    }

    /**
     * 提取关键词并过滤停用词
     * @param string $content 描述
     * @param string $delimiter 切分符号
     * @return array
     */
    public function tokenize(string $content,string $delimiter=','){
        $words = $this->content_to_word($content,$delimiter);
        $stop_words = $this->content_to_word(self::STOP_WORDS,',');
        $diff_words = array_diff($words,$stop_words);
        if($diff_words){
            $diff_words = array_values($diff_words);
            if($this->run_mode == self::MODE_REMARK){
                echo "提取的关键词:[" . implode(',',$diff_words) . "]" . PHP_EOL;
            }
        }else{
            if($this->run_mode == self::MODE_REMARK){
                echo "未提取出关键词" . PHP_EOL;
            }
        }
        return !empty($diff_words) ? $diff_words : [];
    }

    /**
     * 创建锁
     * @param mixed $key
     * @param int $acquire_timeout 请求时长
     * @param int $lock_timeout 锁时效
     * @return bool|string
     */
    public function acquire_lock_with_timeout($key,$acquire_timeout=10,$lock_timeout=10){

        $key = "lock:{$key}";
        $identifier = uniqid();
        $end = microtime(true) + $acquire_timeout;
        while( microtime(true) < $end ){
            if($this->conn->setnx($key,$identifier)){
                $this->conn->expire($key,$lock_timeout);
                return $identifier;
            }else if($this->conn->ttl($key) < 0){
                $this->conn->expire($key,$lock_timeout);
            }
            usleep(1000);
        }
        return false;
    }

    /**
     * 释放锁
     * @param mixed $key
     * @param mixed $value
     * @return bool
     * @throws \Predis\ClientException
     * @throws \Predis\NotSupportedException
     */
    public function release_lock($key,$value){

        $key = "lock:{$key}";
        $trans = $this->conn->transaction(['cas'=>true]);
        while(true){
            try{
                $trans->watch($key);
                if($trans->get($key) == $value){
                    $trans->multi();
                    $trans->del($key);
                    $trans->exec();
                    return true;
                }
                $trans->unwatch();
                break;
            }catch (\Predis\Transaction\AbortedMultiExecException $e){
                //pass
            }
        }

        return false;
    }
}