<?php
/**
 * Created by PhpStorm.
 * User: sbird
 * Date: 2019/10/27
 * Time: 15:55
 */
require("../RedisClient.php");
const STOP_WORDS = " tis, twas, a, able, about, across, after, ain't, all, almost, also, am, among, an, and, any, are, aren't, as, at, be, because, been, but, by, can, can't, cannot, could, could've, couldn't, dear, did, didn't, do, does, doesn't, don't, either, else, ever, every, for, from, get, got, had, has, hasn't, have, he, he'd, he'll, he's, her, hers, him, his, how, how'd, how'll, how's, however, i, i'd, i'll, i'm, i've, if, in, into, is, isn't, it, it's, its, just, least, let, like, likely, may, me, might, might've, mightn't, most, must, must've, mustn't, my, neither, no, nor, not, of, off, often, on, only, or, other, our, own, rather, said, say, says, shan't, she, she'd, she'll, she's, should, should've, shouldn't, since, so, some, than, that, that'll, that's, the, their, them, then, there, there's, these, they, they'd, they'll, they're, they've, this, tis, to, too, twas, us, wants, was, wasn't, we, we'd, we'll, we're, were, weren't, what, what'd, what's, when, when, when'd, when'll, when's, where, where'd, where'll, where's, which, while, who, who'd, who'll, who's, whom, why, why'd, why'll, why's, will, with, won't, would, would've, wouldn't, yet, you, you'd, you'll, you're, you've, your";
$conn = RedisClient::getConn();

function word_index_set($word){
    return sprintf("idx:{$word}",$word);
}

function get_stop_words($len=2){
    $words = [];
    $wordArr = explode(',',STOP_WORDS);
    foreach($wordArr as $k => $w){
        if(strpos($w,' ') !== false){
            $w = str_replace(' ','',$w);
            if(strlen($w) >= $len){
                $words[] = $w;
            }
        }
    }
    return array_unique($words);
}

//提取关键词,过滤停用词
function tokenize($content){
    $words = [];
    $content = strtolower($content);
    foreach(explode(' ',$content) as $w){
        if(!isset($words[$w]) && strlen($w)>=2){
            $words[$w] = $w;
        }
    }
    $stopWords = get_stop_words();
    $ret = array_diff($words,$stopWords);
    if($ret){
        $ret = array_values($ret);
        echo "提取的关键词:[" . implode(',',$ret) . "]" . PHP_EOL;
    }else{
        echo "未提取出关键词" . PHP_EOL;
    }
    return $ret;
}

//创建文章反向索引redis
function index_document($conn,$doc_id,$content){
    $words = tokenize($content);
    if($words){
        echo "开始创建反向索引".PHP_EOL;
        echo "----------------------" . PHP_EOL;
        $responses = $conn->pipeline(function($pipe) use ($words,$doc_id){
            foreach($words as $w){
                $pipe->sadd(word_index_set($w),$doc_id);
                echo "关键词[" . word_index_set($w) . "] , 文章[{$doc_id}]". PHP_EOL;
            }
        });
        $succCnt=0;
        foreach($responses as $status){
            if($status==1){
                $succCnt++;
            }
        }
        echo "----------------------" . PHP_EOL;
        echo "索引添加,成功[$succCnt]条,失败[" . (count($responses) - $succCnt) ."]条" . PHP_EOL;
        return true;
    }

    return false;
}

function _common_store($conn,$method,$keys,$options=[],$ttl=30,$execute=true){

    $id = uniqid();
//    $responses = $conn->pipeline(function($pipe) use ($method,$id,$keys,$ttl){
//        $storeKey = "idx:{$id}";
//        $keyNames = array_map(function($key){
//              return "idx:{$key}";
//        },$keys);
//        call_user_func([$pipe,$method],$storeKey,$keyNames);
//        $pipe->expire($storeKey,$ttl);
//        echo "对[" . (implode(",",$keyNames)) . "], 执行 {$method} ,写入[{$storeKey}] .";
//    });

    $pipe = ($execute) ? $conn->pipeline() : $conn;
    $storeKey = "idx:{$id}";
    $keyNames = array_map(function($key){
        return "idx:{$key}";
    },$keys);
    if($options){
        call_user_func([$pipe,$method],$storeKey,$keyNames,$options);
    }else{
        call_user_func([$pipe,$method],$storeKey,$keyNames);
    }
    $pipe->expire($storeKey,$ttl);
    echo "对[" . (implode(",",$keyNames)) . "], 执行 {$method} ,写入[{$storeKey}] .";
    if($execute){
        $responses = $pipe->execute();
        if(!empty($responses[0]) && !empty($responses[1])){
            echo "执行成功".PHP_EOL;
            return $id;
        }
        echo "执行失败".PHP_EOL;
        return false;
    }

    echo "延后执行" . PHP_EOL;
    return $id;
}

function intersect($conn,$keys,$ttl=30,$execute=true){
    $options = [];
    return _common_store($conn,'sinterstore',$keys,$options,$ttl,$execute);
}
function union($conn,$keys,$ttl=30,$execute=true){
    $options = [];
    return _common_store($conn,'sunionstore',$keys,$options,$ttl,$execute);
}
function difference($conn,$keys,$ttl=30,$execute=true){
    $options = [];
    return _common_store($conn,'sdiffstore',$keys,$options,$ttl,$execute);
}
function zintersect($conn,$keys,$ttl=30,$execute=true){
    $options = [];
    return _common_store($conn,'zinterstore',$keys,$options,$ttl,$execute);
}
function zunion($conn,$keys,$options,$ttl=30,$execute=true){
    return _common_store($conn,'zunionstore',$keys,$options,$ttl,$execute);
}

//query提取关键词
function parse($query){

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

//按关键词搜索文章
function search($conn,$query,$ttl=30){
    list($all,$unwatched) = parse($query);
    if($all){
        echo "同义词, " .json_encode($all) . PHP_EOL;
    }
    if($unwatched){
        echo "排除词, " .json_encode($unwatched) . PHP_EOL;
    }
    $to_intersect = $intersect_result = [];
    foreach($all as $item){
        if(count($item)>1){
            $to_intersect[] = union($conn,$item,$ttl);
        }else{
            $to_intersect[] = $item[0];
        }
    }
    if($to_intersect){
        echo "to_intersect 操作的key [". implode(",",$to_intersect) ."]" .PHP_EOL;
    }
    if(count($to_intersect) > 1){
        $intersect_result = intersect($conn,$to_intersect,$ttl);
    }else{
        $intersect_result = $to_intersect[0];
    }
    if($intersect_result){
        echo "intersect后的key, idx:{$intersect_result}" . PHP_EOL;
    }

    if($unwatched){
        array_unshift($unwatched,$intersect_result);
        echo "unwatched集合,[" . implode(",",$unwatched) . "]" . PHP_EOL;
        $ret = difference($conn,$unwatched,$ttl);
        echo "diff后的key, idx:{$ret}" . PHP_EOL;
        return $ret;
    }

    return $intersect_result;
}

//$doc_list = [
//    1001 => "There are moments in life when you miss someone so much that you just want to pick them from your dreams and hug them for real Dream what you want to dream go where you want to go be what you want to be because you have only one life and one chance to do all the things you want to do",
//];
//停用词列表
//$WORDS_RE = get_stop_words();
//$token = index_document($conn,1001,$doc_list[1001]);

//$ret = intersect($conn,["idx:things","idx:miss"]);
//$ret = union($conn,["idx:things","idx:miss"]);
//$ret = difference($conn,["idx:things","idx:miss"]);
//var_dump($token);

//$ret=parse("connect +connection +disconnect +disconnection chat -proxy -proxies");
//$ret = search($conn,'go +things -dreams',180);
//var_dump($ret);

//foreach([1009,1001,1002,1010] as $n){
//    $time = time();
//    echo "hmset doc{$n} id {$n} created {$time} updated {$time} title article{$n}" .PHP_EOL;
//    sleep(1);
//}
