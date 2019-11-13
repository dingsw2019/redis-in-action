<?php

/**
 * 广告所有的redisKey
 */

class AdRedisKey {
    /**
     * 地理位置
     * @param int $location_id 城市id
     * @structure set 集合
     * @member ad_id  广告id
     * @return string
     */
    public static function location_key($location_id){
        return sprintf("idx:req:%s",$location_id);
    }

    /**
     * 某广告的关键词
     * @param int $ad_id 广告ID
     * @structure set 集合
     * @member word 关键词
     * @return string
     */
    public static function ad_word_key($ad_id){
        return sprintf("terms:%s",$ad_id);
    }

    /**
     * 关键词权重
     * @param string $word 关键词
     * @structure zset 有序集合
     * @member ad_id 广告ID
     * @score  weight 权重值
     * @return string
     */
    public static function word_weight_key($word){
        return sprintf("idx:%s",$word);
    }

    /**
     * 广告类型
     * @structure hash 哈希
     * @field ad_id 广告ID
     * @value ad_type 广告类型
     * @return string
     */
    public static function ad_type_key(){
        return "type:";
    }

    /**
     * CPM
     * @structure zset 有序集合
     * @member ad_id 广告ID
     * @score  cpm   cpm值
     * @return string
     */
    public static function ad_cpm_key(){
        return "ad:base_value:";
    }

    /**
     * eCPM
     * @structure zset 有序集合
     * @member ad_id  广告ID
     * @score  ecpm   ecpm值
     * @return string
     */
    public static function ad_ecpm_key(){
        return "idx:ad:value:";
    }

    /**
     * logId生成器
     * @structure string 字符串
     * @value target_id 自增的logId
     * @return string
     */
    public static function log_id_key(){
        return "ads:served:";
    }

    /**
     * 记录匹配的关键词
     * @param int $target_id logID
     * @structure set 集合
     * @member word  关键词
     * @return string
     */
    public static function log_word_key($target_id){
        return sprintf("terms:matched:%s",$target_id);
    }

    /**
     * 某类广告曝光计数器
     * @param string $ad_type 广告类型
     * @structure string 字符串
     * @value exposure 自增曝光数
     * @return string
     */
    public static function ad_exposure_key($ad_type){
        return sprintf("type:%s:views:",$ad_type);
    }

    /**
     * 某类广告点击计数器
     * @param string $ad_type 广告类型
     * @structure string 字符串
     * @value click 自增点击数
     * @return string
     */
    public static function ad_click_key($ad_type){
        return sprintf("type:%s:clicks:",$ad_type);
    }

    /**
     * 某类广告动作计数器
     * @param string $ad_type 广告类型
     * @structure string 字符串
     * @value action 自增动作数
     * @return string
     */
    public static function ad_action_key($ad_type){
        return sprintf("type:%s:actions:",$ad_type);
    }

    /**
     * 统计曝光的关键词
     * @param string $ad_id 广告ID
     * @structure zset 有序集合
     * @member word 关键词
     * @score  exposure 曝光数
     * @return string
     */
    public static function word_exposure_key($ad_id){
        return sprintf("views:%s",$ad_id);
    }

    /**
     * 统计点击触发的关键词
     * @param string $ad_id 广告ID
     * @structure zset 有序集合
     * @member word 关键词
     * @score  click 点击数
     * @return string
     */
    public static function word_click_key($ad_id){
        return sprintf("clicks:%s",$ad_id);
    }

    /**
     * 统计动作触发的关键词
     * @param string $ad_id 广告ID
     * @structure zset 有序集合
     * @member word 关键词
     * @score  action 动作数
     * @return string
     */
    public static function word_action_key($ad_id){
        return sprintf("actions:%s",$ad_id);
    }
}