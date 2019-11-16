<?php
require_once ("../RedisClient.php");
require_once ("../ch7/search/search.php");

$conn = RedisClient::getConn();

//todo config未完成
function get_redis_connection($component,$wait=1){
    return 1;
}

/* 10.1对SORT搜索分片 start ******************************************************************/

/**
 * 获取已排序的搜索结果
 * @param $query
 * @param null $id
 * @param int $ttl
 * @param string $sort
 * @param int $start
 * @param int $num
 * @return array
 * @throws Exception
 */
function search_get_values($query,$id=null,$ttl=300,$sort='-updated',$start=0,$num=20){
    global $conn;
    $search = new Search();
    //搜索文章并排序
    list($count,$docids,$id) = $search->search_and_sort($query,$id,$ttl,$sort,$start,$num);
    //每篇文章的排序值
    $pipe = $conn->pipeline(['atomic'=>true]);
    $sort = ltrim($sort,'-');
    foreach($docids as $docid){
        $key = SearchRedisKey::doc_info($docid);
        $pipe->hget($key,$sort);
    }
    $sort_column = $pipe->execute();
    //组合文章和排序值
    $data_pairs = [];
    foreach($sort_column as $k => $value){
        $data_pairs[$docids[$k]] = $value;
    }
    return [$count,$data_pairs,$id];
}

/**
 * 在所有分片上执行搜索
 * @param $component
 * @param $shards
 * @param $query
 * @param null $ids
 * @param int $ttl
 * @param string $sort
 * @param int $start
 * @param int $num
 * @param int $wait
 * @return array
 * @throws Exception
 */
function get_shard_results($component,$shards,$query,$ids=null,$ttl=30,$sort='-updated',$start=0,$num=20,$wait=1){

    $count = 0;
    $data = [];
    foreach($shards as $shard){
        //获取redis连接信息
        $conn = get_redis_connection(sprintf("%s:%s",$component,$shard),$wait);
        //获取搜索排序结果
        list($c,$d,$i) = search_get_values($query,$ids[$shard],$ttl,$sort,$start,$num);
        //累计搜索结果数量，结果集，redisKey
        $count += $c;
        array_merge($data,$d);
        $ids[$shard] = $i;
    }
    return [$count,$data,$ids];
}

//分页返回搜索结果
function search_shards($component,$shards,$query,$ids=null,$ttl=30,$sort='-updated',$start=0,$num=20,$wait=1){

    //搜索结果
    list($count,$data,$ids) = get_shard_results($component,$shards,$query,$ids,$ttl,$sort,$start,$num,$wait);
    //搜索结果排序
    $reversed = substr($sort,0,1) === '-' ? true :false;
    if($reversed){
        arsort($data);
    }else{
        asort($data);
    }
    //搜索结果分页
    $results = [];
    foreach(array_slice($data,$start,$start+$num) as $docid => $score){
        $results[] = $docid;
    }

    return [$count,$results,$ids];
}

/* 10.1对SORT搜索分片 end ******************************************************************/

/* 10.2对有序集合搜索分片 start ******************************************************************/
//搜索并返回指定数量的数据
function search_get_zset_values($query,$id=null,$ttl=300,$update=1,$vote=0,$start=0,$num=20,$desc=true){
    global $conn;
    $search = new Search();
    //获取搜索结果
    list($count,$r,$id) = $search->search_and_zsort($query,$id,$ttl,$update,$vote,$start,$num,$desc);
    //按排序取数据
    if($desc){
        $data = $conn->zrevrange($id,0,$start+$num-1,['withscores'=>true]);
    }else{
        $data = $conn->zrange($id,0,$start+$num-1,['withscores'=>true]);
    }
    return [$count,$data,$id];
}

//以有序集合分片的方式获取搜索结果
function search_shards_zset($component,$shards,$query,$ids=null,$ttl=30,$update=1,$vote=0,$start=0,$num=20,$desc=true,$wait=1){

    $count = 0;
    $data = [];
    //搜索所有分片
    foreach($shards as $shard){
        $conn = get_redis_connection(sprintf("%s:%s",$component,$shard),$wait);
        list($c,$d,$i) = search_get_zset_values($query,$ids[$shard],$ttl,$update,$vote,$start,$num,$desc);

        //累计搜索结果集的数量、内容和RedisKey
        $count += $c;
        array_merge($data,$d);
        $ids[$shard] = $i;
    }

    //搜索结果排序
    if($desc){
        arsort($data);
    }else{
        asort($data);
    }

    //分页提取搜索结果
    $results = [];
    foreach(array_slice($data,$start,$num) as $docid => $score){
        $results[] = $docid;
    }

    return [$count,$results,$ids];
}
/* 10.2对有序集合搜索分片 end ******************************************************************/
