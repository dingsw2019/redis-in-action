<?php
/**
 * 短结构
 */
require_once ("../RedisClient.php");

$conn = RedisClient::getConn();

/**
 * 长压缩列表测试
 * @param object $conn
 * @param string $key 列表名
 * @param int $length 列表数量
 * @param $passes
 * @param $psize
 * @return float|int
 * @throws Exception
 */
function long_ziplist_performance($key,$length,$passes,$psize){

    global $conn;

    $conn->del($key);
    $conn->rpush($key,range(0,$length));

    $pipe = $conn->pipeline(['atomic'=>true]);
    $t = microtime(true);
    for($i=0;$i<$passes;$i++){
        for($j=0;$j<$psize;$j++){
            $pipe->rpoplpush($key,$key);
        }
        $pipe->execute();
    }

    return  ($passes * $passes )/ ((microtime(true) - $t) ?: .001);
}

//$p = long_ziplist_performance('list',1,1000,100);//553378.92243533
//echo "length=1,每秒处理元素量为:" . $p . PHP_EOL;
//$p = long_ziplist_performance('list',100,1000,100);//441796.43276818
//echo "length=100,每秒处理元素量为:" . $p . PHP_EOL;
//$p = long_ziplist_performance('list',1000,1000,100);//427900.82520518
//echo "length=1000,每秒处理元素量为:" . $p . PHP_EOL;
//$p = long_ziplist_performance('list',5000,1000,100);//408531.43496542
//echo "length=5000,每秒处理元素量为:" . $p . PHP_EOL;
//$p = long_ziplist_performance('list',10000,1000,100);//443454.64403133
//echo "length=10000,每秒处理元素量为:" . $p . PHP_EOL;
//$p = long_ziplist_performance('list',50000,1000,100);//420656.9515393
//echo "length=50000,每秒处理元素量为:" . $p . PHP_EOL;
//$p = long_ziplist_performance('list',100000,1000,100);//435224.252914
//echo "length=100000,每秒处理元素量为:" . $p . PHP_EOL;

/* 9.2分片结构 start ********************************************************************************/
/**
 * 为元素分配分片ID
 * @param string $base 散列键名
 * @param string $key 要划分的键名
 * @param int $total_elements 元素总量
 * @param int $shard_size 分片数量
 */
function shard_key($base,$key,$total_elements,$shard_size){
    if(is_int($key) or is_float($key) or ctype_digit($key)){
        $shard_id = floor(intval(strval($key),10) / $shard_size);
    }else{
        $shards = 2 * $total_elements / $shard_size;
        $shard_id = crc32($key) % $shards;
    }

    return sprintf('%s:%s',$base,$shard_id);
}

//给纽约分配分片ID
//$shard_key = shard_key("city","New York",370000,5);
//echo "New York 的分片键为:{$shard_key}" .PHP_EOL;

/**
 * 按分片结构设置散列数据
 * @param $base
 * @param $key
 * @param $value
 * @param $total_elements
 * @param $shard_size
 * @return int
 */
function shard_hset($base,$key,$value,$total_elements,$shard_size){
    global $conn;
    $shard = shard_key($base,$key,$total_elements,$shard_size);
    return $conn->hset($shard,$key,$value);
}

/**
 * 按分片结构获取散列值
 * @param $base
 * @param $key
 * @param $total_elements
 * @param $shard_size
 * @return string
 */
function shard_hget($base,$key,$total_elements,$shard_size){
    global $conn;
    $shard = shard_key($base,$key,$total_elements,$shard_size);
    return $conn->hget($shard,$key);
}

/**
 * 日访问者记录
 */

/**
 * 分片方式存储集合
 * @param $base
 * @param $member
 * @param $total_elements
 * @param $shard_size
 * @return int
 */
function shard_add($base,$member,$total_elements,$shard_size){
    global $conn;
    $shard = shard_key($base,'x' . strval($member),$total_elements,$shard_size);
    return $conn->sadd($shard,$member);
}
//默认预估日访客数量
$DAILY_EXPECTED = 10000000;
//本地缓存日访客量
$EXPECTED = [];
const SHARD_SIZE = 512;
/**
 * 日访客数量统计
 * @param $session_id
 * @throws Exception
 */
function count_visit($session_id){
    global $conn;
    //当前统计访客的redisKey
    $today = new DateTime();
    $key = sprintf("unique:%s",$today->format('Y-m-d'));
    //获取当天预计访客数
    $expected = get_expected($key,$today);

    //访客唯一值
    $id = intval(substr(str_replace('-','',$session_id),0,15),16);

    //分片添加集合,日访客人数增加
    if(shard_add($key,$id,$expected,SHARD_SIZE)){
        $conn->incr($key);
    }
}
/**
 * 预估某日访客量
 * @param $key
 * @param $today
 * @return mixed
 */
function get_expected($key,$today){
    global $conn,$DAILY_EXPECTED,$EXPECTED;

    //本地缓存日访客数
    if(array_key_exists($key,$EXPECTED)){
        return $EXPECTED[$key];
    }

    //redis获取日访客数
    $exkey = $key . ":expected";
    $expected = $conn->get($key);

    //预估日访客数
    if(!$expected){
        $yesterday = $today->modify('-1 day');
        $expected = $conn->get(
            sprintf('unique:%s',$yesterday->format('Y-m-d'))
        );
        $expected = intval($expected ?: $DAILY_EXPECTED);
        $expected = 2 ** intval(ceil(log($expected * 1.5, 2)));
        if(!$conn->setnx($exkey,$expected)){
            $expected = $conn->get($exkey);
        }
    }

    $EXPECTED[$key] = intval($expected);
    return $EXPECTED[$key];


}

/* 9.2分片结构 end ********************************************************************************/

/* 9.3打包存储二进制位和字节 start ******************************************************************/
global $COUNTRIES, $STATES;
$COUNTRIES = [
    'ABW', 'AFG', 'AGO', 'AIA', 'ALA', 'ALB', 'AND', 'ARE', 'ARG', 'ARM', 'ASM',
    'ATA', 'ATF', 'ATG', 'AUS', 'AUT', 'AZE', 'BDI', 'BEL', 'BEN', 'BES', 'BFA',
    'BGD', 'BGR', 'BHR', 'BHS', 'BIH', 'BLM', 'BLR', 'BLZ', 'BMU', 'BOL', 'BRA',
    'BRB', 'BRN', 'BTN', 'BVT', 'BWA', 'CAF', 'CAN', 'CCK', 'CHE', 'CHL', 'CHN',
    'CIV', 'CMR', 'COD', 'COG', 'COK', 'COL', 'COM', 'CPV', 'CRI', 'CUB', 'CUW',
    'CXR', 'CYM', 'CYP', 'CZE', 'DEU', 'DJI', 'DMA', 'DNK', 'DOM', 'DZA', 'ECU',
    'EGY', 'ERI', 'ESH', 'ESP', 'EST', 'ETH', 'FIN', 'FJI', 'FLK', 'FRA', 'FRO',
    'FSM', 'GAB', 'GBR', 'GEO', 'GGY', 'GHA', 'GIB', 'GIN', 'GLP', 'GMB', 'GNB',
    'GNQ', 'GRC', 'GRD', 'GRL', 'GTM', 'GUF', 'GUM', 'GUY', 'HKG', 'HMD', 'HND',
    'HRV', 'HTI', 'HUN', 'IDN', 'IMN', 'IND', 'IOT', 'IRL', 'IRN', 'IRQ', 'ISL',
    'ISR', 'ITA', 'JAM', 'JEY', 'JOR', 'JPN', 'KAZ', 'KEN', 'KGZ', 'KHM', 'KIR',
    'KNA', 'KOR', 'KWT', 'LAO', 'LBN', 'LBR', 'LBY', 'LCA', 'LIE', 'LKA', 'LSO',
    'LTU', 'LUX', 'LVA', 'MAC', 'MAF', 'MAR', 'MCO', 'MDA', 'MDG', 'MDV', 'MEX',
    'MHL', 'MKD', 'MLI', 'MLT', 'MMR', 'MNE', 'MNG', 'MNP', 'MOZ', 'MRT', 'MSR',
    'MTQ', 'MUS', 'MWI', 'MYS', 'MYT', 'NAM', 'NCL', 'NER', 'NFK', 'NGA', 'NIC',
    'NIU', 'NLD', 'NOR', 'NPL', 'NRU', 'NZL', 'OMN', 'PAK', 'PAN', 'PCN', 'PER',
    'PHL', 'PLW', 'PNG', 'POL', 'PRI', 'PRK', 'PRT', 'PRY', 'PSE', 'PYF', 'QAT',
    'REU', 'ROU', 'RUS', 'RWA', 'SAU', 'SDN', 'SEN', 'SGP', 'SGS', 'SHN', 'SJM',
    'SLB', 'SLE', 'SLV', 'SMR', 'SOM', 'SPM', 'SRB', 'SSD', 'STP', 'SUR', 'SVK',
    'SVN', 'SWE', 'SWZ', 'SXM', 'SYC', 'SYR', 'TCA', 'TCD', 'TGO', 'THA', 'TJK',
    'TKL', 'TKM', 'TLS', 'TON', 'TTO', 'TUN', 'TUR', 'TUV', 'TWN', 'TZA', 'UGA',
    'UKR', 'UMI', 'URY', 'USA', 'UZB', 'VAT', 'VCT', 'VEN', 'VGB', 'VIR', 'VNM',
    'VUT', 'WLF', 'WSM', 'YEM', 'ZAF', 'ZMB', 'ZWE'
];
$STATES = [
    'CAN' => [
        'AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE', 'QC', 'SK',
        'YT',
    ],
    'USA' => [
        'AA', 'AE', 'AK', 'AL', 'AP', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC',
        'DE', 'FL', 'FM', 'GA', 'GU', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY',
        'LA', 'MA', 'MD', 'ME', 'MH', 'MI', 'MN', 'MO', 'MP', 'MS', 'MT', 'NC',
        'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'PR',
        'PW', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VI', 'VT', 'WA', 'WI',
        'WV', 'WY',
    ],
];

/**
 * 获取国家和洲的编码
 * @param $country
 * @param $state
 * @return int;
 */
function get_code($country,$state){

    global $COUNTRIES, $STATES;
    //获取国家的序号
    $cindex = array_search($country,$COUNTRIES);
    if($cindex>count($COUNTRIES) or $cindex === false){
        $cindex = -1;
    }
    //因为redis给未初始化的数据返回空值，所以设置"未找到国家的"返回0
    $cindex += 1;

    //获取洲的序号
    $sindex = -1;
    if($state and array_key_exists($country,$STATES)){
        $sindex = array_search($state,$STATES[$country]);
        if($sindex>count($STATES[$country]) or $sindex === false){
            $sindex = -1;
        }
    }
    $sindex += 1;
    echo "cindex:{$cindex},sindex:{$sindex}" . PHP_EOL;
    return chr($cindex) . chr($sindex);
}

//用户分片基数
const USERS_PER_SHARD = 2**20;
//设置用户所在城市
function set_location($user_id,$country,$state){
    global $conn;
    //获取国家和洲的编码
    $code = get_code($country,$state);
    //计算分片ID和偏移量
    $shard_id = intdiv($user_id,USERS_PER_SHARD);
    $position = $user_id % USERS_PER_SHARD;
    $offset = $position * 2;
    $pipe = $conn->pipeline(['atomic'=>true]);
    //分片方式写入用户位置
    $pipe->setrange(sprintf("location:%s",$shard_id),$offset,$code);
    //记录最大的用户ID
    $tkey = uniqid();
    $pipe->zadd($tkey,['max'=>$user_id]);
    $pipe->zunionstore('location:max',[$tkey,'location:max'],['aggregate'=>'max']);
    $pipe->del($tkey);
    $pipe->execute();
}

/**
 * 按国家和洲统计用户量
 */
function aggregate_location(){
    global $conn;

    $countries = $states = [];
    $max_id = intval($conn->zscore('location:max','max'));
    $max_block = intval($max_id / USERS_PER_SHARD);

    foreach(range(0,$max_block) as $shard_id){
        foreach(readblocks(sprintf('location:%s',$shard_id)) as $block){
            foreach(range(0,strlen($block)-2,2) as $offset){
                $code = substr($block,$offset,2);
                update_aggregates($countries,$states,[$code]);
            }
        }
    }
}

/**
 * 统计每个国家和洲的用户量
 * @param $countries
 * @param $states
 * @param $codes
 */
function update_aggregates(&$countries,&$states,$codes){
    global $COUNTRIES,$STATES;
    foreach($codes as $code){
        //编码结构检查,是否国家与洲都存在
        if(strlen($codes) != 2){
            continue;
        }
        //提取国家和洲
        $country = ord($codes[0]) - 1;
        $state = ord($codes[1]) - 1;
        //检验国家合法性
        if($country < 0 or $country >= count($COUNTRIES)){
            continue;
        }
        //国家统计量增加
        $country = $COUNTRIES[$country];
        if(!isset($countries[$country])){
            $countries[$country] = 0;
        }
        $countries[$country] += 1;

        //洲合法性校验
        if(!array_key_exists($country,$STATES)){
            continue;
        }
        if($state < 0 or $state >= count($STATES[$country])){
            continue;
        }
        //洲统计量增加
        $state = $STATES[$country][$state];
        if(!isset($states[$country][$state])){
            $states[$country][$state] = 0;
        }
        $states[$country][$state] += 1;
    }


}

/**
 * 统计一组用户的居住分布情况
 * @param $user_ids
 * @return array
 * @throws Exception
 */
function aggregate_location_list($user_ids){
    global $conn;
    $countries = $states = [];

    $pipe = $conn->pipeline(['atomic'=>true]);
    foreach($user_ids as $i => $user_id){
        $shard_id = intdiv($user_id,USERS_PER_SHARD);
        $position = $user_id % USERS_PER_SHARD;
        $offset = $position * 2;
        $pipe->getrange(sprintf('location:%s',$shard_id),$offset,$offset+1);

        if(($i+1) % 1000 == 0){
            update_aggregates($countries,$states,$pipe->execute());
        }
    }

    update_aggregates($countries,$states,$pipe->execute());
    return [$countries,$states];
}

/* 9.3打包存储二进制位和字节 end ********************************************************************/


















