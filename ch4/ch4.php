<?php

require_once ("../RedisClient.php");

$conn = RedisClient::getConn();

/**
 * 日志处理
 * @param string $path 日志文件路径
 * @param $callback
 */
function process_logs($path,$callback){
    global $conn;
    //获取文件的处理进度
    list($current_file,$offset) = $conn->mget(['progress:file','progress:position']);

    $pipe = $conn->pipeline(['atomic'=>true]);
    //更新日志文件的名字和偏移量
    $update_progress = function($fname,$offset) use ($pipe){
        $pipe->mset([
            'progress:file'=>$fname,
            'progress:position'=>$offset,
        ]);
        $pipe->execute();
    };

    foreach(array_diff(scandir($path,SCANDIR_SORT_ASCENDING),['.','..']) as $fname){
        //略过所有已处理的日志文件
        if($fname < $current_file){
            continue;
        }
        //处理因系统崩溃而未能完成处理的日志文件时，略过已处理内容
        $inp = fopen(
            rtrim($path,DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . $fname,
            'rb'
        );
        if($fname == $current_file){
            fseek($inp,intval($offset,10));
        }else{
            $offset = 0;
        }
        $current_file = null;
        //处理日志文件
        $lno = 0;
        while(!feof($inp)) {
            $line = fgets($inp);
            $callback($pipe,$line);
            $offset = intval($offset) + strlen($line);
            $lno++;
            if(!($lno % 1000)) {
                $update_progress($fname,$offset);
            }
        }
        $update_progress($fname,$offset);
        fclose($inp);
    }
}

/**
 * todo zinfo不存在,info不存在aof参数
 * 检查主从数据同步是否完成
 */
function wait_for_sync($mconn,$sconn){

    $identifier = uniqid();
    //将令牌添加到主服务器
    $key = 'sync:wait';
    $mconn->zadd($key,[$identifier=>microtime(true)]);
    //等待从服务器完成同步
    while(!($sconn->zinfo()['master_link_status'] != 'up')){
        usleep(1000);
    }
    //等待从服务器接受数据更新
    while(!$sconn->zscore($key,$identifier)){
        usleep(1000);
    }
    //检查数据更新是否已经被同步到磁盘
    $deadline = microtime(true) + 1.01;
    while(microtime(true) < $deadline){
        if($sconn->info()['aof_pending_bio_fsync'] == 0){
            break;
        }
        usleep(1000);
    }
    //清理令牌
    $mconn->zrem($key,$identifier);
    $mconn->zremrangebyscore($key,0,microtime(true) - 900);
}

/**
 * 商品放到售卖市场
 * @param int $itemid
 * @param int $sellerid
 * @param float $price
 */
function list_item($itemid,$sellerid,$price){
    global $conn;
    //售卖者包裹的redisKey
    $inventory = sprintf("inventory:%s",$sellerid);
    //市场的redisKey
    $market = "market:";
    //市场的member值
    $item = "{$itemid}.{$sellerid}";

    $end = microtime(true) + 5;
    //将cas设置为true,watch和multi之间的操作,在execute之前发送
    $trans = $conn->transaction(['cas'=>true]);

    while (microtime(true) < $end){
        try{
            //监视售卖者包裹
            $trans->watch($inventory);
            //验证用户是否仍然持有指定的商品
            if(!$trans->sismember($inventory,$itemid)){
                //商品不在包裹中,停止监视包裹
                $trans->unwatch();
                return null;
            }
            //将商品添加到售卖市场
            $trans->multi();
            $trans->zadd($market,[$item=>$price]);
            $trans->srem($inventory,$itemid);
            $trans->exec();
            return true;

        }catch (\Predis\Transaction\AbortedMultiExecException $e){
            //pass
        }
    }

    return false;
}

//测试添加商品到售卖市场
//$conn->sadd(sprintf("inventory:%s",1001),['product-A']);
//$conn->sadd(sprintf("inventory:%s",1002),['product-B']);
//$r1 = list_item('product-A',1003,200);
//$r2 = list_item('product-B',1002,10);
//echo "用户1003,售卖product-A的结果：" . ($r1 ? '成功' : '失败') . PHP_EOL;
//echo "用户1002,售卖product-B的结果：" . ($r2 ? '成功' : '失败') . PHP_EOL;


/**
 * 购买商品
 * @param int $buyerid 购买者
 * @param string $itemid 商品
 * @param int $sellerid 售卖者
 * @param float $lprice 价格
 */
function purchase_item($buyerid,$itemid,$sellerid,$lprice){

    global $conn;
    //买家账号
    $buyer = sprintf("users:%s",$buyerid);
    //买家账号
    $seller = sprintf("users:%s",$sellerid);
    //买家的包裹
    $inventory = sprintf("inventory:%s",$buyerid);
    //市场的redisKey
    $market = "market:";
    //市场的member
    $item = "{$itemid}.{$sellerid}";

    $end = microtime(true) + 10;
    $trans = $conn->transaction(['cas'=>true]);

    while (microtime(true) < $end){
        try {
            //监视市场和买家账号
            $trans->watch([$market,$buyer]);
            //检查买家持有资金能否购买商品
            $price = $trans->zscore($market,$item);
            $funds = intval($trans->hget($buyer,'funds'));
            if($price != $lprice OR $price > $funds){
                $trans->unwatch();
                return null;
            }
            //买卖双方开始财物交换
            $trans->multi();
            $trans->hincrby($seller,'funds',intval($price));
            $trans->hincrby($buyer,'funds',intval(-$price));
            $trans->sadd($inventory,$itemid);
            $trans->zrem($market,$item);
            $trans->exec();
            return true;

        }catch (\Predis\Transaction\AbortedMultiExecException $e){
            //pass
        }
    }

    return false;
}

//测试购买商品
//$conn->hset(sprintf("users:%s",1001),"funds",1000);
//$conn->hset(sprintf("users:%s",1002),"funds",10);
//$conn->sadd(sprintf("inventory:%s",1002),['product-B']);
//$r2 = list_item('product-B',1002,500);
//echo "用户1002,售卖product-B的结果：" . ($r2 ? '成功' : '失败') . PHP_EOL;
//$b = purchase_item(1001,'product-B',1002,500);
//echo "用户[1001],购买用户[1002]的商品[product-B]的结果：" . ($b ? '成功' : '失败') . PHP_EOL;