<?php
/**
 * Created by PhpStorm.
 * User: sbird
 * Date: 2019/10/28
 * Time: 21:34
 */

require("search.php");

const AD_TYPE_CPA = 'cpa';
const AD_TYPE_CPC = 'cpc';
const AD_TYPE_CPM = 'cpm';

const TO_ECPM = [
    'cpc' => 'cpc_to_ecpm',
    'cpa' => 'cpa_to_ecpm',
    'cpm' => 'lamb_to_ecpm',
];

//cpm字典 [key:广告类型,value:cpm]
$AVERAGE_PER_1K = [];

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
function clicks_key($ad_id){
    return sprintf("clicks:%s",$ad_id);
}
function actions_key($ad_id){
    return sprintf("actions:%s",$ad_id);
}
function term_target_key($target_id){
    return sprintf("terms:matched:%s",$target_id);
}
function action_counter_key($ad_type){
    return sprintf("type:%s:actions",$ad_type);
}
function click_counter_key($ad_type){
    return sprintf("type:%s:clicks:",$ad_type);
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
 * 按位置匹配广告和ecpm
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

/**
 * 匹配关键词,重新计算ecmp
 * @param $pipe
 * @param $matched
 * @param $base
 * @param $content
 * @return array
 */
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

/**
 * 记录浏览数据
 * @param $conn
 * @param $target_id
 * @param $ad_id
 * @param $words
 */
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

/**
 * 记录点击数据
 * @param $conn
 * @param $target_id
 * @param $ad_id
 * @param bool $action
 */
function record_click($conn,$target_id,$ad_id,$action=false){
    $pipe = $conn->pipeline();

    $click_key = clicks_key($ad_id);
    $match_key = term_target_key($target_id);
    $type = $conn->hget(type_key(),$ad_id);
    //匹配关键词的log数据,延长过期时间
    if($type == AD_TYPE_CPA){
        $pipe->expire($match_key,900);
        if($action){
            $click_key = actions_key($ad_id);
        }
    }

    //类型广告点击计数器
    if($action && $type == AD_TYPE_CPA){
        $pipe->incr(action_counter_key($type));
    }else{
        $pipe->incr(click_counter_key($type));
    }

    //某广告关键词点击计数器
    $words = $conn->smembers($match_key);
    foreach($words as $word){
        $pipe->zincrby($click_key,$word);
    }
    $pipe->execute();
    update_cpms($conn,$ad_id);
}

function update_cpms($conn,$ad_id){
    $pipe = $conn->pipeline();
    //获取广告类型,cpm,关键词
    $pipe->hget(type_key(),$ad_id);
    $pipe->zscore(value_key(),$ad_id);
    $pipe->smembers(term_key(),$ad_id);
    list($type,$base_value,$words) = $pipe->execute();

    //获取广告浏览数和点击数
    $click_key = ($type == AD_TYPE_CPA) ? action_counter_key($type) : click_counter_key($type);
    $pipe->get(views_ad_type_key($type));
    $pipe->get($click_key);
    list($type_views,$type_clicks) = $pipe->execute();

    //计算cpm
    $AVERAGE_PER_1K[$type] = 1000 * intval($type_clicks) / intval($type_views);

    //某广告所有关键词的浏览数和点击数
    $views_key = views_ad_word_key($ad_id);
    $click_key = ($type == AD_TYPE_CPA) ? action_key($ad_id) : click_counter_key($ad_id);
    $to_ecpm = TO_ECPM[$type];
    $pipe->zscore($views_key,' ');
    $pipe->zscore($click_key,' ');
    list($ad_views,$ad_clicks) = $pipe->execute();

    //计算广告ecpm
    if(!$ad_clicks){
        //无点击,使用原ecmp
        $ad_ecmp = $conn->zscore(rvalue_key(),$ad_id);
    }else{
        $ad_ecmp = $to_ecpm($ad_views,$ad_clicks,$base_value);
        $pipe->zadd(rvalue_key(),$ad_ecmp,$ad_id);
    }

    //计算关键词ecpm
    foreach($words as $word){
        //关键词的浏览数和点击数
        $pipe->zscore($views_key,$word);
        $pipe->zscore($click_key,$word);
        list($views,$clicks) = array_slice($pipe->execute(),-1,2,true);
        //无点击不处理
        if(!$clicks){
            continue;
        }
        $word_ecpm = $to_ecpm($views,$clicks,$base_value);
        $bonus = $word_ecpm - $ad_ecmp;
        $pipe->zadd(word_key($word),$bonus,$ad_id);
    }
    $pipe->execute();
}

$ad_id = 123;
$locations = [101,107,103];
$content = "love haha";
//index_ad($conn,$ad_id,$locations,'love ads haha','cpc',100);
//match_location($conn,$locations);
//target_ads($conn,$locations,$content);
