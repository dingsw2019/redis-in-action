<?php
require_once ("../RedisClient.php");
require_once ("./cache.php");

class SessionRedisKey {

    /**
     * 令牌与用户映射
     * @structure hash 哈希
     * @field token 令牌
     * @value uid 用户ID
     * @return string
     */
    public static function login(){
        return 'login:';
    }

    /**
     * 记录令牌最后一次操作的时间
     * @structure zset 有序集合
     * @member token 令牌
     * @score time 操作时间
     * @return string
     */
    public static function recent(){
        return 'recent:';
    }

    /**
     * 用户浏览的商品
     * @param string $token 令牌
     * @structure zset 有序集合
     * @member item 商品ID
     * @score time 浏览时间
     * @return string
     */
    public static function viewed($token){
        return sprintf("viewed:%s",$token);
    }

    /**
     * 用户购物车
     * @param string $session 用户标识
     * @structure hash 哈希
     * @field item 商品ID
     * @value count 商品数量
     * @return string
     */
    public static function cart($session){
        return sprintf("cart:%s",$session);
    }
}

class Session {
    //记录每个用户最近浏览商品的限制长度
    const VIEWED_LENGTH = 25;
    //常驻内容执行标识
    const QUIT = false;
    //session限制长度
//    const SESSION_LENGTH = 10000000;
    const SESSION_LENGTH = 3;

    //redis-cli
    private $conn;

    public function __construct()
    {
        $this->conn = RedisClient::getConn();
    }

    /**
     * 根据令牌获取用户
     * @param string $token 令牌
     * @return string
     */
    public function check_token($token){
        return $this->conn->hget(SessionRedisKey::login(),$token);
    }

    /**
     * 更新用户操作
     * @param string $token 令牌
     * @param mixed $user 用户ID
     * @param null $item 商品ID
     */
    public function update_token($token,$user,$item=NULL){

        $timestamp = microtime(true);
        //设置token与user映射
        $this->conn->hset(SessionRedisKey::login(),$token,$user);
        //记录token最近访问时间
        $this->conn->zadd(SessionRedisKey::recent(),[$token=>$timestamp]);

        if($item){
            //记录用户浏览过的商品
            $this->conn->zadd(SessionRedisKey::viewed($token),[$item=>$timestamp]);
            //保留最近浏览的25个商品记录
            $this->conn->zremrangebyrank(SessionRedisKey::viewed($token),0,-self::VIEWED_LENGTH-1);

            $this->conn->zincrby(CacheRedisKey::viewed(),-1,$item);
        }
    }

    /**
     * 清理session(保持session在指定数量级)
     * 常驻执行
     */
    public function clean_sessions(){
        while(!self::QUIT){

            //是否超过session限制长度
            $size = $this->conn->zcard(SessionRedisKey::recent());
            if($size <= self::SESSION_LENGTH){
                usleep(100000);
                continue;
            }
            //提取要删除的session，一次最多清理100条
            $end_index = min($size - self::SESSION_LENGTH,100);
            $tokens = $this->conn->zrange(SessionRedisKey::recent(),0,$end_index-1);

            //删除购物车、浏览记录、登录记录、最近操作记录
            $session_keys = [];
            foreach($tokens as $token){
                $session_keys[] = SessionRedisKey::viewed($token);
                $session_keys[] = SessionRedisKey::cart($token);
            }
            $this->conn->del($session_keys);
            $this->conn->hdel(SessionRedisKey::login(),$tokens);
            $this->conn->zrem(SessionRedisKey::recent(),$tokens);
        }
    }

    /**
     * 添加/删除购物车的商品
     * @param string $session 用户标识
     * @param int $item 商品ID
     * @param int $count 商品数量
     */
    public function add_to_cart($session,$item,$count){
        if($count<=0){
            $this->conn->hdel(SessionRedisKey::cart($session),[$item]);
        }else{
            $this->conn->hset(SessionRedisKey::cart($session),$item,$count);
        }
    }
}

//$session = new Session();
/**更新用户操作**/
//$session->update_token('token1001',1001,'product111');
//$session->update_token('token1001',1001,'product222');
//$session->update_token('token1002',1002,'product111');
//$session->update_token('token1003',1003,'product222');
//$session->update_token('token1004',1004,'product222');

/**添加购物车**/
//$session->add_to_cart('token1001','product111',10);
//$session->add_to_cart('token1001','product111',3);
//$session->add_to_cart('token1001','product222',2);
//$session->add_to_cart('token1002','product222',2000);
//$session->add_to_cart('token1002','product222',0);

/**清理**/
//$session->clean_sessions();