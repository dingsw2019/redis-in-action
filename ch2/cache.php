<?php
require_once ("../RedisClient.php");

class CacheRedisKey {

    /**
     * 页面缓存
     * @param string $request_code 页面编码
     * @structure string 字符串
     * @value page_content 页面内容
     * @return string
     */
    public static function cache($request_code){
        return sprintf("cache:%s",$request_code);
    }

    /**
     * 商品浏览量
     * @structure zset 有序集合
     * @member item_id 商品ID
     * @score exposure 浏览量
     * @return string
     */
    public static function viewed(){
        return "viewed:";
    }

    /**
     * 延迟执行集合
     * @structure zset 有序集合
     * @member row_id 行ID
     * @score time 延迟时间
     * @return string
     */
    public static function delay(){
        return "delay:";
    }

    /**
     * 执行集合
     * @structure zset 有序集合
     * @member row_id 行ID
     * @score time 执行时间戳
     * @return string
     */
    public static function schedule(){
        return "schedule:";
    }

    /**
     * 存储行数据
     * @param int $row_id 行ID
     * @structure string 字符串
     * @value content 行内容
     * @return string
     */
    public static function inv($row_id){
        return sprintf("inv:%s",$row_id);
    }
}

class Cache {

    const QUIT = false;
    //redis-cli
    private $conn;

    public function __construct()
    {
        $this->conn = RedisClient::getConn();
    }

    /**
     * 缓存页面
     * @param string $request http链接
     * @param mixed $callback 生成页面的回调函数
     * @return string
     */
    public function cache_request($request,$callback){
        //不能缓存的,启动回调函数
        if(!$this->can_cache($request)){
            $callback($request);
        }
        //是否已缓存页面
        $page_key = CacheRedisKey::cache($this->hash_request($request));
        $content = $this->conn->get($page_key);

        //缓存页面
        if(!$content){
            $content = $callback($request);
            $this->conn->setex($page_key,300,$content);
        }

        //返回页面
        return $content;
    }

    /**
     * 判断页面是否需要缓存
     * @param string $request http链接
     * @return bool
     */
    public function can_cache($request){
        //检查是否为商品页面,页面是否能被缓存
        $item_id = $this->extract_item_id($request);
        if(!$item_id OR $this->is_dynamic($request)){
            return false;
        }
        //取商品浏览量排名
        $rank = $this->conn->zrank(CacheRedisKey::viewed(),$item_id);
        //根据浏览量判断是否需要缓存
        return $rank !== null AND $rank < 10000;
    }

    /**
     * 行缓存添加到延迟执行队列和执行队列
     * @param int $row_id 行ID
     * @param int $delay 延迟时间
     */
    public function schedule_row_cache($row_id,$delay){
        $this->conn->zadd(CacheRedisKey::delay(),[$row_id=>$delay]);
        $this->conn->zadd(CacheRedisKey::schedule(),[$row_id=>microtime(true)]);
    }

    /**
     * 处理行缓存
     * 常驻执行
     */
    public function cache_rows(){
        while (!self::QUIT){
            //提取一行
            $next = $this->conn->zrange(CacheRedisKey::schedule(),0,0,['withscores'=>true]);
            $now = microtime(true);
            if(!$next or reset($next) > $now){
                usleep(50000);
                continue;
            }

            $row_id = key($next);
            //执行到时间的延迟任务集合
            $delay = $this->conn->zscore(CacheRedisKey::delay(),$row_id);
            if($delay <= 0){
                $this->conn->zrem(CacheRedisKey::delay(),$row_id);
                $this->conn->zrem(CacheRedisKey::schedule(),$row_id);
                $this->conn->del(CacheRedisKey::inv($row_id));
                continue;
            }

            //更新调度时间并设置缓存值
            $this->conn->zadd(CacheRedisKey::schedule(),[$row_id=>$now+$delay]);
//            $this->set(CacheRedisKey::inv($row_id),json_encode($row->to_dict()));
        }
    }

    /**
     * 定期调整浏览次数和浏览数据量
     */
    public function rescale_viewed(){
        while (!self::QUIT){
            //删除排名20000后的商品
            $this->conn->zremrangebyrank(CacheRedisKey::viewed(),20000,-1);
            //浏览量减半
            $this->conn->zinterstore(CacheRedisKey::viewed(),[CacheRedisKey::viewed()],['weights'=>0.5]);
            sleep(300);
        }
    }

    /**
     * 提取http.query中的item
     * @param string $request http链接
     * @return |null
     */
    private function extract_item_id($request){
        $query = parse_url($request,PHP_URL_QUERY);
        parse_str($query,$query);
        return isset($query['item']) ? $query['item'] : null;
    }

    /**
     * http.query中是否存在"_"
     * @param string $request http链接
     * @return bool
     */
    private function is_dynamic($request){
        $query = parse_url($request,PHP_URL_QUERY);
        parse_str($query,$query);
        return array_key_exists('_',$query);
    }

    /**
     * 请求MD5编码
     * @param string $request http链接
     * @return string
     */
    private function hash_request($request){
        return hash('md5',$request);
    }
}