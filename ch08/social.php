<?php

require_once ("../RedisClient.php");
require_once ("../Common.php");
require_once("../ch06/queue.php");

class SocialRedisKey {

    /**
     * 创建user_info的锁
     * @param $login
     * @structure string
     * @return string
     */
    public static function lock_user_info($login){
        return sprintf("user:%s",$login);
    }

    /**
     * 注册的全部用户
     * @param $login
     * @structure hash
     * @field login
     * @value uid
     * @return string
     */
    public static function user_all(){
        return "users:";
    }

    /**
     * 用户id自增器
     * @structure string
     * @value 用户最大id
     * @return string
     */
    public static function user_id(){
        return "user:id:";
    }

    /**
     * 用户信息表
     * @param int $uid
     * @structure hash
     * @feild login 用户名
     * @feild id 用户标识
     * @feild name 用户昵称
     * @feild followers 关注我的
     * @feild following 我关注的
     * @feild posts 发消息数
     * @feild signup 创建时间
     * @return string
     */
    public static function user_info($uid){
        return sprintf("user:%s",$uid);
    }

    /**
     * 用户消息表
     * @param int $msg_id 消息id
     * @structure hash
     * @field message 消息内容
     * #field posted 发送时间
     * #field id 消息标识
     * #field uid 发送者
     * #field login 用户名
     * @return string
     */
    public static function user_message($msg_id){
        return sprintf("status:%s",$msg_id);
    }

    /**
     * 消息id自增器
     * @structure string
     * @return string
     */
    public static function message_id(){
        return "status:id:";
    }

    /**
     * 用户个人状态消息时间线
     * @param int $uid 用户ID
     * @structure zset 有序集合
     * @member 消息ID
     * @score 创建时间
     * @return string
     */
    public static function user_profile($uid){
        return sprintf("profile:%s",$uid);
    }

    /**
     * 用户主页状态消息时间线
     * @param int $uid 用户ID
     * @structure zset 有序结合
     * @member msg_id 消息ID
     * @score time 创建时间戳
     * @return string
     */
    public static function user_home($uid){
        return sprintf("home:%s",$uid);
    }

    /**
     * 我关注的人
     * @param int $uid 关注者
     * @structure zset 有序集合
     * @member uid 被关注者ID
     * @score time 关注的时间
     * @return string
     */
    public static function following($uid){
        return sprintf("following:%s",$uid);
    }

    /**
     * 关注我的人
     * @param int $uid 被关注者
     * @structure zset 有序集合
     * @member uid 关注者
     * @score time 关注时间
     * @return string
     */
    public static function followers($uid){
        return sprintf("followers:%s",$uid);
    }

    /**
     * 关注组(存用户信息)
     * @param $list_id
     * @structure zset 有序集合
     * @member uid 被关注者
     * @score 关注时间
     * @return string
     */
    public static function list_in($list_id){
        return sprintf("list:in:%s",$list_id);
    }

    /**
     * 用户所在关注组(存组信息)
     * @param $uid
     * @structure zset 有序集合
     * @member list_id 关注组ID
     * @score time 关注的时间
     * @return string
     */
    public static function list_out($uid){
        return sprintf("list:out:%s",$uid);
    }

    /**
     * 组的时间线
     * @param $list_id
     * @structure zset 有序结合
     * @member msg_id 消息ID
     * @score time 创建时间戳
     * @return string
     */
    public static function list_statuses($list_id){
        return sprintf("list:statuses:%s",$list_id);
    }

    /**
     * 组的信息
     * @param $list_id
     * @structure hash 哈希
     * @field following 组人数
     * @return string
     */
    public static function list_info($list_id){
        return sprintf("list:%s",$list_id);
    }
}

class Social {

    //主页时间线最大数据长度
    const HOME_TIMELINE_SIZE = 1000;
    //填充主页的用户列表分页数量
    const REFILL_USERS_STEP = 50;
    //最大用户同步数(用于分批同步状态消息)
    const POSTS_PER_PASS = 1000;
    //redis-cli
    private $conn;

    //class Common
    private $commonClass;

    public function __construct(string $mode = Common::MODE_PURE)
    {
        $this->conn = RedisClient::getConn();
        $this->commonClass = new Common($mode);
    }

    /**
     * 创建用户
     * @param $login
     * @param $name
     * @return bool|int
     * @throws \Predis\ClientException
     * @throws \Predis\NotSupportedException
     */
    public function create_user($login,$name){

        $llogin = strtolower($login);
        //获取写user_info的锁
        $lock_value = $this->commonClass->acquire_lock_with_timeout(SocialRedisKey::lock_user_info($llogin),1);
        //获取锁失败
        if(!$lock_value){
            return false;
        }
        //user是否存在
        $user_exists = $this->conn->hget(SocialRedisKey::user_all(),$llogin);
        if($user_exists){
            $this->commonClass->release_lock(SocialRedisKey::lock_user_info($llogin),$lock_value);
            return false;
        }
        //写user_info
        $id = $this->conn->incr(SocialRedisKey::user_id());
        $pipe = $this->conn->pipeline();
        $pipe->hmset(SocialRedisKey::user_all(),[$llogin=>$id]);
        $pipe->hmset(SocialRedisKey::user_info($id),[
            "login" => $llogin,
            "id" => $id,
            "name" => $name,
            "followers" => 0,
            "following" => 0,
            "posts" => 0,
            "signup" => time(),

        ]);
        $pipe->execute();
        $this->commonClass->release_lock(SocialRedisKey::lock_user_info($llogin),$lock_value);
        return $id;
    }

    /**
     * 创建状态消息
     * @param int $uid 用户ID
     * @param string $message 消息内容
     * @param $data
     * @return mixed
     * @throws Exception
     */
    private function create_status($uid,$message,$data){
        //获取login,生成消息ID
        list($login,$id) = $this->conn->pipeline(function($pipe) use ($uid){
            $pipe->hget(SocialRedisKey::user_info($uid),'login');
            $pipe->incr(SocialRedisKey::message_id());
        });
        //写消息
        $data = [
            'message' => $message,
            'posted' => time(),
            'id'=>$id,
            'uid'=>$uid,
            'login'=>$login
        ];
        $pipe = $this->conn->pipeline();
        $pipe->hmset(SocialRedisKey::user_message($id),$data);
        //用户发送消息数增加
        $pipe->hincrbyfloat(SocialRedisKey::user_info($uid),'posts',1);
        $pipe->execute();
        //返回消息ID
        return $id;
    }

    /**
     * 获取个人消息主页
     * @param int $uid 用户ID
     * @param int $page 页码
     * @param int $count 单页数据量
     * @return array
     * @throws Exception
     */
    public function get_status_message($uid,int $page=1,int $count=30){
        $start = ($page-1) * $count;
        $stop = $page*$count-1;
        //获取主页消息(分页)
        $messages_ids = $this->conn->zrevrange(SocialRedisKey::user_home($uid),$start,$stop);
        //获取消息详情
        $pipe = $this->conn->pipeline(['atomic'=>true]);
        foreach($messages_ids as $message_id){
            $pipe->hgetall(SocialRedisKey::user_message($message_id));
        }
        $messages = $pipe->execute();

        //返回
        return array_filter($messages);
    }

    /**
     * 关注
     * @param int $uid  关注者
     * @param int $other_uid 被关注者
     */
    public function follow_user($uid,$other_uid){
        $fkey1 = SocialRedisKey::following($uid);
        $fkey2 = SocialRedisKey::followers($other_uid);

        //是否已关注
        if($this->conn->zscore($fkey1,$other_uid)){
            return false;
        }
        $pipe = $this->conn->pipeline(['atomic'=>true]);
        //关注者名单与被关注者名单添加
        $pipe->zadd($fkey1,[$other_uid=>time()]);
        $pipe->zadd($fkey2,[$uid=>time()]);
        //获取被关注者最近1000条个人消息
        $pipe->zrevrange(SocialRedisKey::user_profile($other_uid),0,self::HOME_TIMELINE_SIZE-1,['withscores'=>true]);
        list($following,$followers,$status_and_scores) = $pipe->execute();

        $pipe = $this->conn->pipeline(['atomic'=>true]);
        //用户信息的关注数与被关注数修改 todo following不是true false吧？
        $pipe->hincrby(SocialRedisKey::user_info($uid),"following",intval($following));
        $pipe->hincrby(SocialRedisKey::user_info($other_uid),"followers",intval($followers));
        //主页时间线添加被关注人消息
        if($status_and_scores){
            $pipe->zadd(SocialRedisKey::user_home($uid),$status_and_scores);
        }
        //主页时间线只保留最新的1000条
        $pipe->zremrangebyrank(SocialRedisKey::user_home($uid),0,-self::HOME_TIMELINE_SIZE-1);
        $pipe->execute();
        return true;
    }

    /**
     * 取消关注
     * @param int $uid 关注者
     * @param int $other_uid 被关注者
     */
    public function unfollow_user($uid,$other_uid){
        $fkey1 = SocialRedisKey::following($uid);
        $fkey2 = SocialRedisKey::followers($other_uid);
        //是否关注
        if(!$this->conn->zscore($fkey1,$other_uid)){
            return false;
        }
        $pipe = $this->conn->pipeline(['atomic'=>true]);
        //删除关注者名单和被关注者名单
        $pipe->zrem($fkey1,$other_uid);
        $pipe->zrem($fkey2,$uid);
        //获取被关注者的消息
        $pipe->zrevrange(SocialRedisKey::user_profile($other_uid),0,self::HOME_TIMELINE_SIZE-1);
        list($following,$followers,$statuses) = $pipe->execute();

        $pipe = $this->conn->pipeline(['atomic'=>true]);
        //删除用户的关注数和被关注数修改
        $pipe->hincrby(SocialRedisKey::user_info($uid),"following",-intval($following));
        $pipe->hincrby(SocialRedisKey::user_info($other_uid),"followers",-intval($followers));
        //删除被关注者在主页时间线的内容
        if($statuses){
            $pipe->zrem(SocialRedisKey::user_home($uid),$statuses);
        }
        $pipe->execute();
        return true;
    }

    /**
     * 分片延迟填充主页时间线
     * @param string $incoming 获取填充消息的用户ID
     * @param string $timeline 被填充的主页时间线的redisKey
     */
    public function refill_timeline($incoming,$timeline){
        $queue = new Queue();

        //主页消息数>750才可填充
        if($this->conn->zcard($timeline) > 750){
            return ;
        }
        //用户数量切片
        $users_count = ceil($this->conn->zcard($incoming) / self::REFILL_USERS_STEP);
        for($batch=0;$batch<$users_count;$batch++){
            $start = self::REFILL_USERS_STEP * ($batch-1);
            $stop = $start + self::REFILL_USERS_STEP - 1;
            //延迟处理
            $id = $queue->execute_later('default','refill_timeline_in_range',
                [$incoming,$timeline,$start,$stop]);
            echo "id={$id}" . PHP_EOL;
        }
    }

    /**
     * 填充主页时间线
     * @param string $incoming 获取填充消息的用户ID
     * @param string $timeline 被填充的主页时间线的redisKey
     * @param $start
     * @param $stop
     * @throws Exception
     */
    public function refill_timeline_in_range($incoming,$timeline,$start,$stop){

        //范围获取用户
        $users = $this->conn->zrange($incoming,$start,$stop);
        $pipe = $this->conn->pipeline();
        //获取每个用户的最近1000个消息
        foreach($users as $uid){
            $pipe->zrevrange(SocialRedisKey::user_profile($uid),0,self::HOME_TIMELINE_SIZE,['withscores'=>true]);
        }

        //合并消息,取最近1000条
        $messages = [];
        foreach($pipe->execute() as $results){
            $messages += $results;
        }
        arsort($messages);
        $messages = array_slice($messages,0,self::HOME_TIMELINE_SIZE,true);

        //填充消息，保留最近1000条
        if($messages){
            $pipe = $this->conn->pipeline(['atomic'=>true]);
            $pipe->zadd($timeline,$messages);
            $pipe->zremrangebyrank($timeline,0,-self::HOME_TIMELINE_SIZE-1);
            $pipe->execute();
        }
        echo "填充完成" . PHP_EOL;
    }

    /**
     * 关注(分组方式)
     * @param int $uid 关注者
     * @param int $other_id 被关注者
     * @param int $list_id 组ID
     * @return bool|void
     * @throws Exception
     */
    public function follow_user_list($uid,$other_id,$list_id){

        //组内成员(存放关注的人)
        $fkey1 = SocialRedisKey::list_in($list_id);
        //用户所在分组
        $fkey2 = SocialRedisKey::list_out($other_id);

        //是否关注
        if($this->conn->zscore($fkey1,$other_id)){
            return ;
        }

        $pipe = $this->conn->pipeline();
        $now = time();
        //添加我关注的和关注我的,获取被关注者的最近1000条消息
        $pipe->zadd($fkey1,[$other_id=>$now]);
        $pipe->zadd($fkey2,[$list_id=>$now]);
        $pipe->zcard($fkey1);
        $pipe->zrevrange(SocialRedisKey::user_profile($other_id),0,self::HOME_TIMELINE_SIZE,['withscores'=>true]);
        list($following,$status_and_score) = array_slice($pipe->execute(),-2);

        //添加组的时间线和组关注数
        $pipe = $this->conn->pipeline();
        $pipe->hset(SocialRedisKey::list_info($list_id),"following",$following);
        if($status_and_score){
            $pipe->zadd(SocialRedisKey::list_statuses($list_id),$status_and_score);
        }
        //只保留最近1000条数据
        $pipe->zremrangebyrank(SocialRedisKey::list_statuses($list_id),0,-self::HOME_TIMELINE_SIZE-1);
        $pipe->execute();
        return true;
    }

    /**
     * 取消关注(分组方式)
     * @param $uid
     * @param $other_id
     * @param $list_id
     * @return bool|void
     * @throws Exception
     */
    public function unfollow_user_list($uid,$other_id,$list_id){
        //组内成员(存放关注的人)
        $fkey1 = SocialRedisKey::list_in($list_id);
        //用户所在分组
        $fkey2 = SocialRedisKey::list_out($other_id);

        //未关注检查
        if(!$this->conn->zscore($fkey1,$other_id)){
            return ;
        }

        $pipe = $this->conn->pipeline(['atomic'=>true]);
        $now = time();
        //删除我的关注和关注我的
        $pipe->zrem($fkey1,$other_id);
        $pipe->zrem($fkey2,$list_id);
        $pipe->zcard($fkey1);
        //获取被关注者所有状态消息
        $pipe->zrevrange(SocialRedisKey::user_profile($other_id),0,self::HOME_TIMELINE_SIZE-1);
        list($following,$statuses) = array_slice($pipe->execute(),-2);

        $pipe = $this->conn->pipeline(['atomic'=>true]);
        //减少组关注者数量
        $pipe->hset(SocialRedisKey::list_info($list_id),"following",$following);
        //在组时间线中删除被关注者所有状态消息
        if($statuses){
            $pipe->zrem(SocialRedisKey::list_statuses($list_id),$statuses);
            //填充内容
            $this->refill_timeline($fkey1,SocialRedisKey::list_statuses($list_id));
        }
        $pipe->execute();
        return true;

    }

    /**
     * 发送状态消息
     * @param int $uid
     * @param string $message
     * @param $data
     * @return mixed|null
     * @throws Exception
     */
    public function post_status($uid,$message,$data){
        //创建消息
        $id = $this->create_status($uid,$message,$data);
        if(!$id){
            return null;
        }
        //获取创建消息时间
        $post = $this->conn->hget(SocialRedisKey::user_message($id),"posted");
        if(!$post){
            return null;
        }
        //消息加入个人状态消息时间线
        $status_data = [$id=>$post];
        $this->conn->zadd(SocialRedisKey::user_profile($uid),$status_data);

        //消息加入关注者的状态消息时间线
        $this->syndicate_status($uid,$status_data);
        return $id;
    }

    /**
     * 分批同步状态消息给关注我的人
     * @param int $uid 发消息的人
     * @param array $post 消息ID和创建消息时间
     */
    public function syndicate_status($uid,$post){
        //用户分段
        $followers_count = $this->conn->zcard(SocialRedisKey::followers($uid));
        $batches = ceil($followers_count/self::POSTS_PER_PASS);

        $queue = new Queue();
        for($batch=0;$batch<$batches;$batch++){
            //计算开始结束
            $start = $batch*self::POSTS_PER_PASS;
            $stop = $start + self::POSTS_PER_PASS - 1;
            if($batch == 0){
                $this->syndicate_status_in_range($uid,$post,$start,$stop);
            }else{
                //1000之后的延迟处理
                $queue->execute_later("default","syndicate_status_in_range",
                    [$uid,$post,$start,$stop]);
            }
        }
    }

    /**
     * 给一批人同步状态消息
     * @param int $uid 发消息的人
     * @param array $post 消息ID和创建消息时间
     * @param int $start 关注我的人搜索范围起点
     * @param int $stop 关注我的人搜索范围终点
     * @throws Exception
     */
    private function syndicate_status_in_range($uid,$post,$start,$stop){

        //获取关注我的人
        $followers = $this->conn->zrange(SocialRedisKey::followers($uid),$start,$stop);
        //给关注的人添加状态消息,保持状态消息在1000条
        $pipe = $this->conn->pipeline(['atomic'=>true]);
        foreach($followers as $follower){
            $pipe->zadd(SocialRedisKey::user_home($follower),$post);
            $pipe->zremrangebyrank(SocialRedisKey::user_home($follower),0,-self::HOME_TIMELINE_SIZE-1);
        }
        $pipe->execute();
    }

    /**
     * 关注组同步消息
     * @param int $uid
     * @param array $post
     * @throws Exception
     */
    public function syndicate_status_list($uid,$post){

        $lists_id = $this->conn->zrange(SocialRedisKey::list_out($uid),0,'inf');
        $pipe = $this->conn->pipeline();
        foreach($lists_id as $list_id){
            $pipe->zadd(SocialRedisKey::list_statuses($list_id),$post);
            $pipe->zremrangebyrank(SocialRedisKey::list_statuses($list_id),0,-self::HOME_TIMELINE_SIZE-1);
        }
        $pipe->execute();
    }

    /**
     * 删除消息
     * @param $uid
     * @param $msg_id
     * @return bool|null
     * @throws \Predis\ClientException
     * @throws \Predis\NotSupportedException
     */
    public function delete_message($uid,$msg_id){

        $key = SocialRedisKey::user_message($msg_id);
        //申请锁
        $locked = $this->commonClass->acquire_lock_with_timeout($key,1);
        if(!$locked){
            return null;
        }
        //检查用户是消息的创建者
        $msg_info = $this->conn->hgetall(SocialRedisKey::user_message($msg_id));
        if(!$msg_info or $msg_info['uid']!=$uid){
            $this->commonClass->release_lock($key,$locked);
            return false;
        }

        $pipe = $this->conn->pipeline();
        //删除消息
        $pipe->del(SocialRedisKey::user_message($msg_id));
        //删除个人状态消息表的某消息
        $pipe->zrem(SocialRedisKey::user_profile($uid),$msg_id);
        //删除主页时间线的某消息
        $pipe->zrem(SocialRedisKey::user_home($uid),$msg_id);
        //减少用户信息的发送消息数
        $pipe->hincrby(SocialRedisKey::user_info($uid),"posts",-1);
        $pipe->execute();

        $this->commonClass->release_lock($key,$locked);

        $this->clean_timelines($uid,$msg_id);
        return true;
    }

    /**
     * 清除关注者的某个状态消息
     * @param int $uid 消息创建者
     * @param int $msg_id 消息ID
     */
    public function clean_timelines($uid,$msg_id){
        $followers_count = $this->conn->zcard(SocialRedisKey::followers($uid));
        $batches = ceil($followers_count / self::POSTS_PER_PASS);
        $queue = new Queue();
        for($batch=0;$batch<$batches;$batch++){
            $start = $batch * self::POSTS_PER_PASS;
            $stop = $start + self::POSTS_PER_PASS - 1;
            if($batch == 0){
                $this->clean_timelines_in_range($uid,$msg_id,$start,$stop);
            }else{
                $queue->execute_later("default","clean_timelines_in_range",
                    [$uid,$msg_id,$start,$stop]);
            }
        }
    }

    /**
     * 批量清除关注者的某个状态消息
     * @param int $uid
     * @param int $msg_id
     * @param int $start
     * @param int $stop
     * @throws Exception
     */
    private function clean_timelines_in_range($uid,$msg_id,$start,$stop){

        //获取关注者
        $followers = $this->conn->zrange(SocialRedisKey::followers($uid),$start,$stop);
        //删除关注者的某个消息
        $pipe = $this->conn->pipeline();
        foreach($followers as $follower){
            $pipe->zrem(SocialRedisKey::user_home($follower),$msg_id);
        }
        $pipe->execute();
    }
}

//$social = new Social();
//$social->unfollow_user_list(1,2,3);

//写消息
//$social->post_status(1,'message content',3);

//获取我的主页时间线
//$statuses_data = $social->get_status_message(2);
//var_dump($statuses_data);

//删除消息
//$social->delete_message(1,ch7);