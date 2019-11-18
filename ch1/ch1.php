<?php

require_once ("../RedisClient.php");

class ArticleRedisKey{

    /**
     * 文章基础数据
     * @param int $article_id
     * @structure hash 哈希
     * @field title    标题
     * @field link     链接
     * @field poster   作者
     * @field time     创建时间
     * @field votes    赞数
     * @return string
     */
    public static function article_info(int $article_id){
        return sprintf("article:%s",$article_id);
    }

    /**
     * 文章自增ID
     * @structure string 字符串
     * @value article_id 最大文章ID
     * @return string
     */
    public static function article_id(){
        return "article:";
    }

    /**
     * 所有文章的评分
     * @structure zset 有序集合
     * @member article_id 文章ID
     * @score score 分数 (赞+创建时间)
     * @return string
     */
    public static function article_score(){
        return "score:";
    }

    /**
     * 记录每篇文章投票的用户
     * @param $article_id
     * @structure set 集合
     * @member user_id 用户ID
     * @return string
     */
    public static function voted($article_id){
        return sprintf("voted:%s",$article_id);
    }

    /**
     * 所有文章的创建时间
     * @structure zset 有序集合
     * @member article_id 文章ID
     * @score time 创建时间戳
     * @return string
     */
    public static function article_time(){
        return "time:";
    }

    /**
     * 文章组
     * @param string $group 组名
     * @structure set 集合
     * @member article_id 文章ID
     * @return string
     */
    public static function group($group){
        return sprintf("group:%s",$group);
    }
}

class Article{

    //一周的秒数
    const ONE_WEEK_IN_SECONDS = 7 * 86400;
    //一次投票的分值,计算公式：86400 / 200 = 432
    const VOTE_SCORE = 432;
    //文章列表每页显示数据量
    const ARTICLES_PER_PAGE = 25;
    //redis-cli
    private $conn;

    public function __construct()
    {
        $this->conn = RedisClient::getConn();
    }

    /**
     * 投票
     * @param int $user 投票用户
     * @param string $article 文章
     */
    public function article_vote($user,$article){

        //是否可投票检查
        $cutoff = microtime(true) - self::ONE_WEEK_IN_SECONDS;
        if($this->conn->zscore(ArticleRedisKey::article_time(),$article) < $cutoff){
            return ;
        }

        $partition = explode(':',$article);
        $article_id = end($partition);
        //已投票检查
        if($this->conn->sadd(ArticleRedisKey::voted($article_id),[$user])) {
            //文章赞数、评分增加
            $this->conn->zincrby(ArticleRedisKey::article_score(),self::VOTE_SCORE,$article);
            $this->conn->hincrby(ArticleRedisKey::article_info($article_id),'votes',1);
        }
    }

    /**
     * 发布文章
     * @param int $user 作者
     * @param string $title 标题
     * @param string $link 链接
     * @return int
     */
    public function post_article(int $user,string $title,string $link){

        //获取文章ID
        $article_id = $this->conn->incr(ArticleRedisKey::article_id());

        //作者自投票,设置投票过期时间
        $this->conn->sadd(ArticleRedisKey::voted($article_id),$user);
        $this->conn->expire(ArticleRedisKey::voted($article_id),self::ONE_WEEK_IN_SECONDS);

        //写入文章
        $now = microtime(true);
        $article = ArticleRedisKey::article_info($article_id);
        $this->conn->hmset($article,[
                  'title'=>$title,    //标题
                  'link'=>$link,    //链接
                  'poster'=>$user,    //作者
                  'time'=>$now,    //创建时间
                  'votes'=>1,    //赞数
        ]);

        //写入文章评分、创建时间索引
        $this->conn->zadd(ArticleRedisKey::article_score(),[$article=>$now+self::VOTE_SCORE]);
        $this->conn->zadd(ArticleRedisKey::article_time(),[$article=>$now]);
        //返回文章数
        return $article_id;
    }

    /**
     * 获取文章列表
     * @param int $page 页码
     * @return array
     */
    public function get_articles(int $page,string $order=null){
        if(!$order){
            $order = ArticleRedisKey::article_score();
        }
        //获取偏移量和单页数据量
        $start = ($page - 1) * self::ARTICLES_PER_PAGE;
        $end = $start + self::ARTICLES_PER_PAGE - 1;
        //按评分获取文章ID
        $ids = $this->conn->zrevrange($order,$start,$end);
        //获取文章完整数据
        $articles = [];
        foreach($ids as $id){
            $articles[$id] = $this->conn->hgetall($id);
        }
        //返回文章数据
        return array_filter($articles);
    }

    /**
     * 添加/删除分组的文章
     * @param int $article_id 文章ID
     * @param array $to_add
     * @param array $to_remove
     */
    public function add_remove_groups(int $article_id,array $to_add = [], array $to_remove = []){
        $article = 'article:' . $article_id;
        foreach($to_add as $group){
            $this->conn->sadd(ArticleRedisKey::group($group),[$article]);
        }
        foreach($to_remove as $group){
            $this->conn->srem(ArticleRedisKey::group($group),[$article]);
        }
    }

    /**
     * 按组获取文章并按评分排序
     * @param string $group
     * @param int $page
     * @return array
     */
    public function get_group_articles(string $group,int $page){
        //存组文章的临时key
        $key = sprintf("score:%s",$group);
        if(!$this->conn->exists($key)){
            $this->conn->zinterstore($key,[ArticleRedisKey::group($group),ArticleRedisKey::article_score()],['aggregate'=>'max']);
            $this->conn->expire($key,60);
        }

        return $this->get_articles($page,$key);
    }
}

$article = new Article();
//发布文章
//$article->post_article(1001,'php','php.net');
//$article->post_article(1002,'redis','redis.com');
//$article->post_article(1003,'python','python.com');

//文章投票
//$article->article_vote(1004,"article:1");
//$article->article_vote(1002,"article:3");

//获取文章列表
//$articles = $article->get_articles(1);
//var_dump($articles);

//添加分组
//$article->add_remove_groups(1,['language']);
//$article->add_remove_groups(3,['language']);

//获取分组
//$articles = $article->get_group_articles("language",1);
//var_dump($articles);