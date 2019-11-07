<?php
/**
 * 广告类注释版,输出redis命令,中文注释
 * todo redis的SortSet,存储float精度不准,会导致idx:ad:value:的score出错
 */
require_once("../../RedisClient.php");
require_once("../Common.php");
require_once("AdRedisKey.php");

use ad\Common;
use ad\AdRedisKey;

class AdClassPure {

    //ecpm基数
    const ECPM_BASE_NUMBER = 1000;
    //曝光触发ecpm更新的频率
    const UPDATED_EXPOSURE_NUMBER = 10;
    //自增频率
    const INCR_VALUE = 1;
    //某广告曝光、点击、动作数的member值
    const AD_EXPOSURE_MEMBER = ' ';
    const AD_CLICK_MEMBER = ' ';
    const AD_ACTION_MEMBER = ' ';
    //过期时间
    const TTL_900 = 900;

    //广告类型
    const AD_TYPE_CPA = 'cpa';
    const AD_TYPE_CPC = 'cpc';
    const AD_TYPE_CPM = 'cpm';

    //广告类型字典
    const AD_TYPE_DICT = [
        self::AD_TYPE_CPA,
        self::AD_TYPE_CPC,
        self::AD_TYPE_CPM,
    ];

    //计算ecpm的方法
    const TO_ECPM = [
        'cpc' => 'cpc_to_ecpm',
        'cpa' => 'cpa_to_ecpm',
        'cpm' => 'cpm_to_ecpm',
    ];

    //千次展示的点击数 [key:广告类型,value:点击数]
    private $AVERAGE_PER_1K = [
        self::AD_TYPE_CPA => 100,
        self::AD_TYPE_CPC => 200,
        self::AD_TYPE_CPM => 300
    ];
    //redis-cli
    private $conn;
    //class Common
    private $commonClass;

    public function __construct(string $mode)
    {
        $this->conn = RedisClient::getConn();
        $this->commonClass = new Common($mode);
    }

    /**
     * cpc千次展示收益
     * @param int $view 浏览数
     * @param int $clicks 点击数
     * @param float $cpc cpc单价
     * @return float|int
     */
    private function cpc_to_ecpm($view,$clicks,$cpc){
        return round(self::ECPM_BASE_NUMBER * $cpc * $clicks/$view,2);
    }

    /**
     * cpa千次展示收益
     * @param int $view 浏览数
     * @param int $clicks 点击数
     * @param float $cpa cpa单价
     * @return float|int
     */
    private function cpa_to_ecpm($view,$actions,$cpa){
        return round(self::ECPM_BASE_NUMBER * $cpa * ($actions/$view), 2);
    }
    /**
     * cpm千次展示收益
     * @param int $view 浏览数
     * @param int $clicks 点击数
     * @param float $cpa cpm单价
     * @return float|int
     */
    private function cpm_to_ecpm($view,$actions,$cpm){
        return $cpm;
    }

    /**
     * 按位置匹配广告和ecpm
     * @param object $pipe redis管道
     * @param array $locations 位置ID
     * @return array
     */
    private function match_location($pipe,array $locations){
        $keys = array_map(function($location){
            return AdRedisKey::location_key($location);
        },$locations);
        //匹配广告
        $matched_ads = $this->commonClass->sunionstore($pipe,$keys,[],Common::DEFAULT_TTL,Common::EXECUTE_FALSE);
        //以上广告的ecpm
        $matched_ecpm = $this->commonClass->zinterstore($pipe,[$matched_ads,AdRedisKey::ad_ecpm_key()],[],Common::DEFAULT_TTL,Common::EXECUTE_FALSE);
        return [$matched_ads,$matched_ecpm];
    }

    /**
     * 提取关键词、计算ecpm
     * @param object $pipe redis管道
     * @param string $matched_ad 匹配的广告的redisKey
     * @param string $matched_ecpm 匹配的广告的ecpm的redisKey
     * @param string $content 广告的描述
     * @return array
     */
    private function finish_scoring($pipe,$matched_ad,$matched_ecpm,$content){
        //提取关键词,过滤停用词
        $words = $this->commonClass->tokenize($content);
        //匹配关键词权重
        $matched_word_weight = [];
        foreach($words as $word){
            $matched_word_weight[] = $this->commonClass->zinterstore($pipe,[$matched_ad,AdRedisKey::word_weight_key($word)],[],Common::DEFAULT_TTL,Common::EXECUTE_FALSE);
        }
        //重新计算ecpm
        if($matched_word_weight){
            $min = $this->commonClass->zunionstore($pipe,$matched_word_weight,['aggregate'=>'min'],Common::DEFAULT_TTL,Common::EXECUTE_FALSE);
            $max = $this->commonClass->zunionstore($pipe,$matched_word_weight,['aggregate'=>'max'],Common::DEFAULT_TTL,Common::EXECUTE_FALSE);
            $matched_ecpm = $this->commonClass->zunionstore($pipe,[$matched_ecpm,$min,$max],['weights'=>[1,0.5,0.5]],Common::DEFAULT_TTL,Common::EXECUTE_FALSE);
        }

        return [$words,$matched_ecpm];
    }

    /**
     * 广告入库
     * @param int $ad_id 广告ID
     * @param string $ad_type 广告类型
     * @param array $locations 位置ID
     * @param string $content 广告描述
     * @param int $cpm cpm值
     */
    public function index_ad($ad_id,$ad_type,$locations,$content,$cpm){

        $pipe = $this->conn->pipeline();
        //地理位置
        foreach($locations as $location){
            $pipe->sadd(AdRedisKey::location_key($location),[$ad_id]);
        }
        //关键词权重
        $words = $this->commonClass->tokenize($content);
        foreach($words as $word){
            $pipe->zadd(AdRedisKey::word_weight_key($word),[$ad_id=>0]);
        }
        //广告类型
        $pipe->hset(AdRedisKey::ad_type_key(),$ad_id,$ad_type);
        //cpm
        $pipe->zadd(AdRedisKey::ad_cpm_key(),[$ad_id=>$cpm]);
        //ecpm
        $ecpm = call_user_func([$this,self::TO_ECPM[$ad_type]],self::ECPM_BASE_NUMBER,$this->AVERAGE_PER_1K[$ad_type] ?? 1,$cpm);
        $pipe->zadd(AdRedisKey::ad_ecpm_key(),[$ad_id=>$ecpm]);
        //广告的关键词
        if($words){
            $pipe->sadd(AdRedisKey::ad_word_key($ad_id),$words);
        }

        return $pipe->execute();
    }

    /**
     * 匹配广告
     * @param array $locations 位置id
     * @param string $content  广告上下文内容
     * @return array
     * @throws Exception
     */
    public function target_ads(array $locations,string $content){

        $pipe = $this->conn->pipeline();
        //匹配广告和ecpm
        list($matched_ads,$matched_ecpm) = $this->match_location($pipe,$locations);
        //提取关键词,重新计算ecpm
        list($words,$targeted_ads) = $this->finish_scoring($pipe,$matched_ads,$matched_ecpm,$content);
        //获取广告id和ecpm
        $pipe->incr(AdRedisKey::log_id_key());
        $pipe->zrevrange($targeted_ads,0,0);
        list($target_id,$targeted_ad) = array_slice($pipe->execute(),-2,2);

        $ret = [NULL,NULL];
        if($targeted_ad){
            $ad_id = $targeted_ad[0];
            //记录曝光数据
            $this->record_targeting_result($target_id,$ad_id,$words);
            $ret = [$target_id,$ad_id];
        }
        return $ret;
    }


    /**
     * 记录曝光数据
     * @param $target_id
     * @param $ad_id
     * @param $words
     */
    private function record_targeting_result($target_id,$ad_id,$words){

        $pipe = $this->conn->pipeline();
        //记录某类广告曝光数
        $ad_type = $this->conn->hget(AdRedisKey::ad_type_key(),$ad_id);
        $pipe->incr(AdRedisKey::ad_exposure_key($ad_type));
        //记录匹配的关键词,记录关键词的曝光数和某广告曝光数
        $ad_words = $this->conn->smembers(AdRedisKey::ad_word_key($ad_id));
        $matched_words = array_intersect($words,$ad_words);
        if($matched_words){
            $pipe->sadd(AdRedisKey::log_word_key($target_id),$matched_words);
            $pipe->expire(AdRedisKey::log_word_key($target_id),self::TTL_900);
            foreach($matched_words as $word){
                $pipe->zincrby(AdRedisKey::word_exposure_key($ad_id),self::INCR_VALUE,$word);
            }
            $pipe->zincrby(AdRedisKey::word_exposure_key($ad_id),self::INCR_VALUE,self::AD_EXPOSURE_MEMBER);
        }
        list($ad_exposure) = array_slice($pipe->execute(),-1,1);
        //每100次曝光,触发一次ecpm更新
        if(($ad_exposure % self::UPDATED_EXPOSURE_NUMBER) === 0){
            $this->update_cpms($ad_id);
        }
    }

    /**
     * 记录点击/动作数据
     * @param $target_id
     * @param $ad_id
     * @param bool $action
     */
    public function record_click($target_id,$ad_id){

        $pipe = $this->conn->pipeline();
        $ad_type = $this->conn->hget(AdRedisKey::ad_type_key(),$ad_id);
        list($type_key,$word_key) = ($ad_type == self::AD_TYPE_CPA) ?
            [AdRedisKey::ad_action_key($ad_type),AdRedisKey::word_action_key($ad_id)] :
            [AdRedisKey::ad_click_key($ad_type),AdRedisKey::word_click_key($ad_id)];

        //cpa类型延长log过期时间
        if($ad_type == self::AD_TYPE_CPA){
            $pipe->expire(AdRedisKey::log_word_key($target_id),self::TTL_900);
        }

        //记录某类广告点击/动作数
        $pipe->incr($type_key);

        //记录关键词点击/动作数和某广告点击/动作数
        $words = $this->conn->smembers(AdRedisKey::log_word_key($target_id));
        foreach($words as $word){
            $pipe->zincrby($word_key,self::INCR_VALUE,$word);
        }
        $pipe->zincrby($word_key,self::INCR_VALUE,self::AD_CLICK_MEMBER);
        $pipe->execute();

        $this->update_cpms($ad_id);
    }

    /**
     * 计算ecpm
     * @param $ad_id
     */
    private function update_cpms($ad_id){

        //获取广告类型,cpm,关键词
        list($ad_type,$ad_cpm,$words) = $this->conn->pipeline()
            ->hget(AdRedisKey::ad_type_key(),$ad_id)
            ->zscore(AdRedisKey::ad_cpm_key(),$ad_id)
            ->smembers(AdRedisKey::ad_word_key($ad_id))
            ->execute();

        //计算千次点击数(根据某类广告的曝光数和点击数)
        $click_key = ($ad_type == self::AD_TYPE_CPA) ? AdRedisKey::ad_action_key($ad_type) : AdRedisKey::ad_click_key($ad_type);
        list($type_exposure,$type_click) = $this->conn->pipeline()
            ->get(AdRedisKey::ad_exposure_key($ad_type))
            ->get($click_key)
            ->execute();
        $this->AVERAGE_PER_1K[$ad_type] = intval(self::ECPM_BASE_NUMBER * ($type_click ?: 1) / ($type_exposure ?: 1));

        //计算某广告的ecpm(根据曝光和点击)
        $word_click_key = ($ad_type == self::AD_TYPE_CPA) ? AdRedisKey::word_action_key($ad_id) : AdRedisKey::word_click_key($ad_id);
        list($ad_exposure,$ad_click) = $this->conn->pipeline()
            ->zscore(AdRedisKey::word_exposure_key($ad_id),self::AD_EXPOSURE_MEMBER)
            ->zscore($word_click_key,self::AD_CLICK_MEMBER)
            ->execute();

        $pipe = $this->conn->pipeline();
        if(!$ad_click){
            $ad_ecpm = $this->conn->zscore(AdRedisKey::ad_ecpm_key(),$ad_id);
        }else{
            $ad_ecpm = call_user_func([$this,self::TO_ECPM[$ad_type]],$ad_exposure ?: 1,$ad_click ?: 0,$ad_cpm);
            $pipe->zadd(AdRedisKey::ad_ecpm_key(),[$ad_id=>$ad_ecpm]);
        }

        //计算各关键词的权重(广告ecpm与关键词ecpm的差)
        foreach($words as $word){
            list($word_exposure,$word_click) = $this->conn->pipeline(function ($pipe) use ($ad_id,$word,$word_click_key){
                $pipe->zscore(AdRedisKey::word_exposure_key($ad_id),$word);
                $pipe->zscore($word_click_key,$word);
            });
            $word_ecpm = call_user_func([$this,self::TO_ECPM[$ad_type]],$word_exposure ?: 1,$word_click ?: 0,$ad_cpm);
            $weight = $word_ecpm - $ad_ecpm;
            $word_weight[$word] = ($weight>0) ? $weight : 0;
        }
        if($word_weight){
            $this->conn->pipeline(function($pipe) use ($ad_id,$word_weight){
                foreach($word_weight as $word => $weight){
                    $pipe->zadd(AdRedisKey::word_weight_key($word),[$ad_id=>$weight]);
                }
            });
        }
    }
}