<?php
/**
 * 测试广告类,医疗类广告匹配,根据地理位置、身体部位匹配广告
 */
require("./AdClassRemark.php");
require("./AdClassPure.php");

//地理位置
const BEIJING = 1001;
const TIANJIN = 1002;
const SHANGHAI = 1003;
const GUANGZHOU = 1004;
const TAIYUAN = 1005;
const QINGDAO = 1006;

//身体部位
const TEETH = "teeth";//牙齿
const EYE = "eye";//眼睛
const EAR = "ear";//耳朵
const KNEE = "knee";//膝盖
const LUNG = "lung";//肺
const WRIST = "wrist";//手腕


//随机cpm(0-1之间)
function rand_cpm(){
    return round(lcg_value(),4);
}

//随机广告类型
function rand_ad_type(){
    $type_dict = AdClassRemark::AD_TYPE_DICT;
    shuffle($type_dict);
    return AdClassRemark::AD_TYPE_DICT[array_rand($type_dict)];
}

/**
 * 创建广告、曝光、点击数据
 * @param array $ads 广告数据(广告ID,地理位置,分类),如"广告池"展示数据结构
 * @param string $mode 运行模式 [remark,pure]注释版或纯净版, 注释版会输出redis命令数据
 * @throws Exception
 */
function run(array $ads,string $mode){

    if($mode == Common::MODE_REMARK){
        $adClass = new AdClassRemark($mode);
    }else{
        $adClass = new AdClassPure($mode);
    }

    //创建广告
    foreach($ads as $ad){
        $adClass->index_ad($ad['id'],rand_ad_type(),$ad['location'],implode(",",$ad['category']),rand_cpm());
    }

    foreach($ads as $ad){
        $target_list = [];
        //匹配广告,随机曝光
        $exposure = mt_rand(1,10);
        for($i=0;$i<$exposure;$i++){
            list($target_id,$ad_id) = $adClass->target_ads($ad['location'],implode(",",$ad['category']));
            $target_list[$target_id] = $ad_id;
        }
        //点击广告,随机点击
        $click = mt_rand(1,$exposure);
        echo "" . PHP_EOL;
        $target_ids = array_rand($target_list,$click);
        if(!is_array($target_ids)){
            $target_ids = [$target_ids];
        }
        foreach($target_ids as $target_id){
            $adClass->record_click($target_id,$target_list[$target_id]);
        }

        $modeStr = ($mode == Common::MODE_REMARK) ? "注释" : "纯净";
        echo "DONE,{$modeStr}版模式运行,曝光数[{$exposure}],点击数[{$click}]" . PHP_EOL;
    }
}

//广告池
$ads = [
    ["id"=>1,"location"=>[BEIJING],"category"=>[TEETH,EYE,EAR]],
//    ["id"=>2,"location"=>[BEIJING],"category"=>[TEETH,WRIST,EAR]],
//    ["id"=>3,"location"=>[TIANJIN],"category"=>[KNEE,WRIST]],
//    ["id"=>4,"location"=>[SHANGHAI],"category"=>[TEETH,WRIST,EAR]],
//    ["id"=>5,"location"=>[GUANGZHOU],"category"=>[LUNG,EAR]],
//    ["id"=>6,"location"=>[GUANGZHOU],"category"=>[KNEE,WRIST]],
//    ["id"=>7,"location"=>[GUANGZHOU],"category"=>[WRIST,EAR]],
];

//注释版运行
//run($ads,\ad\Common::MODE_REMARK);
//纯净版运行
//run($ads,\ad\Common::MODE_PURE);