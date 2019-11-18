<?php

require_once ("../RedisClient.php");

$conn = RedisClient::getConn();

/**
 * 列表保持浏览商品数
 * @param $token
 * @param $user
 * @param null $item
 */
function update_token($token,$user,$item=null){
    global $conn;
    $timestamp = microtime(true);
    $conn->hset('login:',$token,$user);
    $conn->zadd('recent:',[$token=>$timestamp]);
    if($item){
        $key = 'viewed:' . $token;
        $conn->lrem($key,1,$item);
        $conn->rpush($key,$item);
        $conn->ltrim($key,-25,-1);
    }
    $conn->zincrby('viewed:',-1,$item);
}

//测试浏览商品数
//foreach(range(1,30) as $id){
//    update_token('token1001',1001,'product'.$id);
//}

const ONE_WEEK_IN_SECONDS = 7 * 86400;
const VOTE_SCORE = 432;
/**
 * 投票
 * @param $user
 * @param $article
 * @throws Exception
 */
function article_vote($user,$article){
    global $conn;
    $cutoff = microtime(true) - ONE_WEEK_IN_SECONDS;
    $posted = $conn->zscore('time:',$article);
    if($posted < $cutoff){
        return ;
    }

    $partition = explode(':',$article);
    $article_id = end($partition);
    $pipeline = $conn->pipeline();
    $pipeline->sadd(sprintf("voted:%s",$article_id),$user);
    $pipeline->expire(sprintf("voted:%s",$article_id),intval($posted-$cutoff));
    if (array_slice($pipeline->execute(),-1,1)){
        $pipeline->zincrby('score:',VOTE_SCORE,$article);
        $pipeline->hincrby($article,'votes',1);
        $pipeline->execute();
    }
}

function article_vote_v2($user,$article){
    global $conn;
    $cutoff = microtime(true) - ONE_WEEK_IN_SECONDS;
    $posted = $conn->zscore('time:',$article);
    $partition = explode(':',$article);
    $article_id = end($partition);
    $voted = 'voted:' . $article_id;

    $pipeline = $conn->pipeline();
    while($posted > $cutoff) {
        try{
            $pipeline->watch($voted);
            if(!$pipeline->sismember($voted,$user)){
                $pipeline->multi();
                $pipeline->sadd($voted,$user);
                $pipeline->expire($voted,intval($posted - $cutoff));
                $pipeline->zincrby('score:',VOTE_SCORE,$article);
                $pipeline->hincrby($article,'votes',1);
                $pipeline->execute();
            }
        }catch (\Predis\PredisException $e){
            $cutoff = microtime(true) - ONE_WEEK_IN_SECONDS;
        }
    }
}

const ARTICLES_PER_PAGE = 25;
function get_articles($page,$order='score:'){
    global $conn;
    $start = max($page-1,0) * ARTICLES_PER_PAGE;
    $end = $start + ARTICLES_PER_PAGE;
    $ids = $conn->zrevrange($order,$start,$end);

    $pipeline = $conn->pipeline();
    foreach($ids as $id){
        $pipeline->hgetall($id);
    }

    $articles = [];
    foreach(array_combine($ids,$pipeline->execute()) as $id => $article_data){
        $article_data['id'] = $id;
        $articles[] = $article_data;
    }
    return $articles;
}

const THIRTY_DAYS = 30 * 86400;
function check_token($token){
    global $conn;
    return $conn->get(sprintf('login:%s',$token));
}

function update_token_v2($token,$user,$item=null){
    global $conn;
    $conn->setex(sprintf("login:%s",$token),$user,THIRTY_DAYS);
    $key = sprintf('viewed:%s',$token);
    if($item){
        $conn->lrem($key,1,$item);
        $conn->rpush($key,$item);
        $conn->ltrim($key,-25,-1);
    }
    $conn->expire($key,THIRTY_DAYS);
    $conn->zincrby('viewed:',-1,$item);
}

function add_to_cart($session,$item,$count){
    global $conn;
    $key = sprintf('cart:%s',$session);
    if($count <= 0){
        $conn->hdel($key,[$item]);
    }else{
        $conn->hset($key,$item,$count);
    }
    $conn->expire($key,THIRTY_DAYS);
}