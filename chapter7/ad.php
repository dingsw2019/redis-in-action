<?php
/**
 * Created by PhpStorm.
 * User: sbird
 * Date: 2019/10/28
 * Time: 21:34
 */

require("search.php");

const TO_ECPM = [
    'cpc' => 'cpc_to_ecpm',
    'cpa' => 'cpa_to_ecpm',
    'cpm' => 'lamb_to_ecpm',
];

function location_key($location_id){
    return sprintf("idx:req:%s",$location_id);
}
function word_key($word){
    return sprintf("idx:%s",$word);
}
function type_key(){
    return "type:";
}
function rvalue_key(){
    return "idx:ad:value:";
}
function value_key(){
    return "ad:base_value:";
}
function ad_served_key(){
    return "ads:served:";
}

function term_key($ad_id){
    return sprintf("terms:%s",$ad_id);
}
function views_ad_type_key($ad_type){
    return sprintf("type:%s:views:",$ad_type);
}
function views_ad_word_key($ad_id){
    return sprintf("views:%s",$ad_id);
}

function cpc_to_ecpm($view,$clicks,$cpc){
    return 1000 * $cpc * ($clicks/$view);
}
function cpa_to_ecpm($view,$actions,$cpa){
    return 1000 * $cpa * ($actions/$view);
}
/**
 * 广告入库
 * @param $conn
 * @param int $id 广告ID
 * @param array $locations 位置id
 * @param string $content 广告内容
 * @param string $type 广告类型
 * @param $value
 */
function index_ad($conn,$id,$locations,$content,$type,$value){

    $pipe = $conn->pipeline();

    //地理位置
    foreach($locations as $loc){
        $pipe->sadd(location_key($loc),$id);
    }

    //关键词
    $words = tokenize($content);
    foreach($words as $w){
        $pipe->zadd(word_key($w),0,$id);
    }

    //计算ecpm
//    $rvalue = TO_ECPM[$type](1000,AVERAGE_PER_1K[$type],$value);
    $rvalue = 1;
    $pipe->hset(type_key(),$id,$type);//类型记录(hash)
    $pipe->zadd(rvalue_key(),$rvalue,$id);//ecpm记录(zset)
    $pipe->zadd(value_key(),$value,$id);//原值记录(zset)
    $pipe->sadd(term_key($id),$words);//关键词记录(set)
    $pipe->execute();
}

/**
 * 匹配广告
 * @param $conn
 * @param array $locations
 * @param string $content
 */
function target_ads($conn,$locations,$content){
    $pipe = $conn->pipeline();
    //按地理位置匹配广告和ecpm
    list($matchedAds,$baseEcpm) = match_location($pipe,$locations);
    //
    list($words,$targeted_ads) = finish_scoring($pipe,$matchedAds,$baseEcpm,$content);

    //广告匹配id生成器
    $pipe->incr(ad_served_key());
    //获取ecpm最高的广告
    $pipe->zrevrange('idx:'.$targeted_ads,0,0);
    $exec = $pipe->execute();
    //广告id & 广告匹配id
    $targeted_ad = array_pop($exec);
    $target_id = array_pop($exec);

    if(!$targeted_ad){
        return [null,null];
    }

    $ad_id = $targeted_ad[0];
    record_targeting_result($conn,$target_id,$ad_id,$words);

    return [$target_id,$ad_id];
}

/**
 * 按位置匹配广告,找到所有广告,再交集value
 * @param $pipe
 * @param $locations
 * @return array
 */
function match_location($pipe,$locations){
    //全部位置的广告union
    $keys = array_map(function($loc){
        return "req:" . $loc;
    },$locations);
    $matched_ads = union($pipe,$keys,300,false);
    $matched_values = zintersect($pipe,[$matched_ads,'ad:value:'],300,false);
    return [$matched_ads,$matched_values];
}

function finish_scoring($pipe,$matched,$base,$content){
    $bonus_ecpm = [];
    $words = tokenize($content);
    foreach($words as $word){
        $word_bonus = zintersect($pipe,[$matched,$word],300,false);
        $bonus_ecpm[$word_bonus] = 1;
    }

    if($bonus_ecpm){
        $min = zunion($pipe,$bonus_ecpm,['aggregate'=>'min'],300,false);
        $max = zunion($pipe,$bonus_ecpm,['aggregate'=>'max'],300,false);
        $ecpmUnion = zunion($pipe,[$base,$min,$max],[],300,false);
        return [$words,$ecpmUnion];
    }

    return [$words,$base];
}

function record_targeting_result($conn,$target_id,$ad_id,$words){

    $pipe = $conn->pipeline();
    //匹配某广告的关键词
    $terms = $conn->smembers(term_key($ad_id));
    $matched = array_intersect($words,$terms);
    if($matched){
        //记录关键词
        $matched_key = sprintf("terms:matched:%s",$target_id);
        $pipe->sadd($matched_key,$matched);
        $pipe->expire($matched_key,900);
    }

    //广告类型浏览数+1
    $type = $conn->hget(type_key(),$ad_id);
    $pipe->incr(views_ad_type_key($type));
    //某广告的关键词浏览数+1
    foreach($matched as $word){
        $pipe->zincrby(views_ad_word_key($ad_id),$word);
    }
    //某广告浏览计数器
    $pipe->zincrby(views_ad_word_key($ad_id),'');

    //某广告每浏览100次,重新计算cpm
    $ad_record_count = array_pop($pipe->execute());
    if($ad_record_count % 100){
//        update_cpms($conn,$ad_id);
    }
}

function record_click($conn,$target_id,$ad_id,$action=false){
    $pipeline = $conn->pipeline();
    $click_key = sprintf('clicks:%s',$ad_id);
    $match_key ='';
}

$ad_id = 123;
$locations = [101,107,103];
$content = "love haha";
//index_ad($conn,$ad_id,$locations,'love ads haha','cpc',100);
//match_location($conn,$locations);
//target_ads($conn,$locations,$content);
