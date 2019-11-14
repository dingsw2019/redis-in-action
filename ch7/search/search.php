<?php

require_once("../../RedisClient.php");
require_once("../../Common.php");

class SearchRedisKey {

    /**
     * 关键词索引
     * @param string $word 关键词
     * @structure set 集合
     * @member doc_id 文章ID
     * @return string
     */
    public static function index_word(string $word){
        return sprintf("idx:%s",$word);
    }

    /**
     * @param int $doc_id
     * @structure hash 散列
     * @field id 文章ID
     * @field created 创建时间戳
     * @field updated 更新时间戳
     * @field title 文章标题
     * @return string
     */
    public static function doc_info(int $doc_id){
        return sprintf("kb:doc%s",$doc_id);
    }

    /**
     * 文章更新时间
     * @structure zset
     * @member doc_id 文章ID
     * @score time 更新时间戳
     * @return string
     */
    public static function doc_update(){
        return sprintf("doc:update");
    }

    /**
     * 文章的投票数
     * @structure zset
     * @member doc_id 文章ID
     * @score vote 投票数
     * @return string
     */
    public static function doc_vote(){
        return sprintf("doc:vote");
    }
}

class Search {

    //redis-cli
    private $conn;
    //class Common
    private $commonClass;
    //string to score mapping
    public $LOWER;
    public $ALPHA;
    public $LOWER_NUMERIC;
    public $ALPHA_NUMERIC;

    public function __construct(string $mode = Common::MODE_PURE)
    {
        $this->conn = RedisClient::getConn();
        $this->commonClass = new Common($mode);

        $ATOZ = range(ord('A'),ord('Z'));
        $atoz = range(ord('a'),ord('z'));
        $numeric = range(ord('0'),ord('9'));
        //ascii与自定义序号的映射表
        $this->LOWER = $this->to_char_map(array_merge($atoz,[-1]));//小写字母
        $this->ALPHA = $this->to_char_map(array_merge($ATOZ,$atoz,[-1]));//大小写字母
        $this->LOWER_NUMERIC = $this->to_char_map(array_merge($numeric,$atoz,[-1]));//数字与小写字母
        $this->ALPHA_NUMERIC = $this->to_char_map(array_merge($numeric,$ATOZ,$atoz,[-1]));//数字与大小写字母
    }

    /**
     * 设置文章信息(用于搜索排序)
     * @param int $doc_id
     * @param string $title
     * @return mixed
     */
    private function set_document(int $doc_id,string $title){
        $data = [
            'id' => $doc_id,
            'updated' => time(),
            'title' => $title
        ];
        $exists = $this->conn->exists(SearchRedisKey::doc_info($doc_id));
        if(!$exists){
            $data['created'] = time();
        }
        return $this->conn->hmset(SearchRedisKey::doc_info($doc_id),$data);
    }

    /**
     * 创建文档反向索引
     * @param int $doc_id
     * @param string $content
     * @return bool
     */
    public function index_document(int $doc_id,string $content){
        $words = $this->commonClass->tokenize($content,' ');
        if($words){
            //设置文章信息
            $this->set_document($doc_id,"title{$doc_id}");
            $pipe = $this->conn->pipeline(['atomic'=>true]);
            foreach($words as $word){
                $pipe->sadd(SearchRedisKey::index_word($word),[$doc_id]);
            }
            $pipe->execute();
            return true;
        }
        return false;
    }

    /**
     * 解析搜索词,提取搜索词和排除词
     * @param string $query 搜索词
     * @return array
     */
    private function parse(string $query){

        $queryList = explode(" ",$query);
        $current = $unwatched = [];
        foreach($queryList as $word){
            $prefix = $word[0];
            if($prefix == "+" || $prefix == "-"){
                $word = substr($word,1);
            }else{
                $prefix = NULL;
            }
            if($prefix == "-"){
                $unwatched[$word] = $word;
                continue;
            }
            if($current && !$prefix){
                $all[] = array_values($current);
                $current = [];
            }
            $current[$word] = $word;
        }
        if($current){
            $all[] = array_values($current);
        }

        return [$all,array_values($unwatched)];
    }

    /**
     * 关键词搜索
     * @param string $query 关键词
     * @param int $ttl 临时redisKey时效
     * @return bool|mixed|string|null
     */
    public function parse_and_search(string $query,int $ttl = 30){
        //提取搜索词和排除词
        list($all,$unwatched) = $this->parse($query);
        if(!$all){
            return null;
        }
        //并集同类搜索词
        $to_intersect = [];
        foreach($all as $syn){
            if(count($syn)>1){
                $syn = array_map(function($word){
                    return SearchRedisKey::index_word($word);
                },$syn);
                $to_intersect[] = $this->commonClass->sunionstore($this->conn,$syn,$ttl);
            }else{
                $to_intersect[] = SearchRedisKey::index_word($syn[0]);
            }
        }
        //交集搜索词
        if(count($to_intersect)>1){
            $intersect_result = $this->commonClass->sinterstore($this->conn,$to_intersect,$ttl);
        }else{
            $intersect_result = $to_intersect[0];
        }
        //去掉排除词
        if($unwatched){
            $unwatched = array_map(function($word){
                return SearchRedisKey::index_word($word);
            },$unwatched);
            array_unshift($unwatched,$intersect_result);
            return $this->commonClass->sdiffstore($this->conn,$unwatched,$ttl);
        }

        return $intersect_result;
    }

    /**
     * 搜索排序
     * @param string $query 搜索词
     * @param mixed $id 存储搜索结果的redisKey
     * @param int $ttl 搜索结果的有效时长
     * @param string $sort 排序字段
     * @param int $start limit的偏移量
     * @param int $num 单页数据量
     * @return array
     */
    public function search_and_sort($query,$id=null,$ttl=30,$sort="-updated",$start=0,$num=20){
        //排序方式(升序、降序)
        $desc = strpos($sort,'-')===0;
        $sort = ltrim($sort,'-');
        //关联项
        $by = "kb:doc:*->" . $sort;
        //排序规则(自然排序)
        $alpha = !in_array($sort,['updated','id','created']);

        //搜索结果是否存在
        if($id and !$this->conn->expire($id,$ttl)){
            $id = null;
        }

        //获取搜索结果
        if(!$id){
            $id = $this->parse_and_search($query);
        }

        $pipe = $this->conn->pipeline(['atomic'=>true]);
        //搜索结果数量
        $pipe->scard($id);
        $options = [
            'BY' => $by,
            'LIMIT'=>[$start,$num],
            'SORT' => $desc ? 'DESC' : 'ASC',
            'ALPHA' => $alpha
        ];
        //搜索排序
        $pipe->sort($id,$options);
        list($search_count,$search_sort) = $pipe->execute();
        return [$search_count,$search_sort,$id];
    }

    /**
     * 搜索复合排序
     * @param string $query 搜索词
     * @param mixed $id 存储搜索结果的redisKey
     * @param int $ttl 搜索结果的有效时长
     * @param int $update 更新时间的排序权重
     * @param int $vote 投票数的排序权重
     * @param int $start limit的偏移量
     * @param int $num 单页数据量
     * @param bool $desc 升降序
     */
    public function search_and_zsort($query,$id=null,$ttl=30,$update=1,$vote=1,$start=0,$num=20,$desc=true){

        //搜索结果是否存在
        if($id && !$this->conn->expire($id,$ttl)){
            $id = null;
        }

        //获取搜索结果,排序因子组合
        if(!$id){
            $id = $this->parse_and_search($query);
            $id = $this->commonClass->zinterstore($this->conn,[$id,SearchRedisKey::doc_update(),SearchRedisKey::doc_vote()],['weights'=>[0,$update,$vote]],$ttl);
        }
        
        //获取搜索结果数量,排序结果
        $pipe = $this->conn->pipeline(['atomic'=>true]);
        $pipe->zcard($id);
        if($desc){
            $pipe->zrevrange($id,$start,$start+$num-1);
        }else{
            $pipe->zrange($id,$start,$start+$num-1);
        }
        list($search_count,$search_data) = $pipe->execute();
        //返回搜索结果数量,排序结果,搜索结果ID
        return [$search_count,$search_data,$id];
    }

    /**
     * 字符串转换为数字
     * @param string $str 需转换的字符串
     * @param bool $ignore_case 是否小写转换
     * @return string
     */
    public function string_to_score(string $str,bool $ignore_case=false){
        if($ignore_case){
            $str = strtolower($str);
        }
        $pieces = array_map('ord',str_split(substr($str,0,6)));
        while (count($pieces) < 6) {
            array_push($pieces,-1);
        }
        $score = 0;
        foreach($pieces as $piece){
            $score = $score * 257 + $piece + 1;
        }

        return $score * 2 + (strlen($str) > 6);
    }

    //kv转换
    public function to_char_map($set){
        $out = [];
        sort($set);
        foreach($set as $pos => $val){
            $out[$val] = $pos - 1;
        }
        return $out;
    }

    /**
     * 通用字符转换分值
     * @param string $str 转换字符
     * @param array $mapping 字符库(大小写字母和数字)
     * @return bool|float|int
     */
    public function string_to_score_generic(string $str,array $mapping){
        $length = intval(52 / log(count($mapping),2));
        $pieces = array_map('ord',str_split(substr($str,0,$length)));
        while(count($pieces) < $length){
            array_push($pieces,-1);
        }

        $score = 0;
        foreach($pieces as $piece){
            $value = $mapping[$piece];
            $score = $score * count($mapping) + $value + 1;
        }

        return $score * 2 + (strlen($str) > $length);
    }
}



$doc_list = [
    1001 => "There are moments in life when you miss someone so much that you just want to pick them from your dreams and hug them for real Dream what you want to dream go where you want to go be what you want to be because you have only one life and one chance to do all the things you want to do",
    1002 => "miss the furthest distance in the world Is not between life and death But when I stand in front of you Yet you don't know that I love you",
    1003 => "miss The furthest distance in the world Is not when I stand in front of you Yet you can't see my love But when undoubtedly knowing the love from both Yet cannot be together"
];
$search = new Search();

//创建文档索引
//foreach($doc_list as $doc_id => $content){
//    $search->index_document($doc_id,$content);
//}

//关键词搜索
//$query = "one +moments -much";
//$key = $search->parse_and_search($query);
//$result = RedisClient::getConn()->smembers($key);
//var_dump($result);

//搜索排序(倒叙)
//list($search_count,$search_data,$search_id) = $search->search_and_sort("miss");
//echo "搜索结果的数量:{$search_count} , 用于排序的搜索Key:[{$search_id}], 以下为排序结果" . PHP_EOL;
//var_dump($search_data);

//更新时间和投票数的复合排序(倒叙)
//RedisClient::getConn()->zadd(SearchRedisKey::doc_update(),[
//    1001=>"1573642268",
//    1002=>"1573642244",
//    1003=>"1573642311",
//]);
//RedisClient::getConn()->zadd(SearchRedisKey::doc_vote(),[
//    1001=> 123,//1573642391
//    1002=> 321,//1573642565
//    1003=> 213,//1573642524
//]);
//list($search_count,$search_data,$search_id) = $search->search_and_zsort("miss");
//echo "搜索结果的数量:{$search_count} , 用于排序的搜索Key:[{$search_id}], 以下为排序结果" . PHP_EOL;
//var_dump($search_data);

//字符转换分值(固定6位长度)
//$str = 'abc';
//$score = $search->string_to_score($str);
//echo "字符串:[{$str}],转换分值为:[{$score}]" . PHP_EOL;

//字符转换分值(通用)
//$str = 'abc';
//$score = $search->string_to_score_generic($str,$search->ALPHA_NUMERIC);
//echo "字符串:[{$str}],转换分值为:[{$score}]" . PHP_EOL;


