<?php

require_once("../../RedisClient.php");
require_once("../Common.php");

use ad\Common;
/**
 * 职位业务所需redisKey
 */
class JobRedisKey {

    /**
     * 职位的技能
     * @param int $job_id 职位ID
     * @structure set 集合
     * @member skill 技能
     * @return string
     */
    public static function job(int $job_id){
        return sprintf("job:%s",$job_id);
    }

    /**
     * 个人的技能(临时表)
     * 避免并发,应给key加入uid,如 temp:$uid
     * @structure set 集合
     * @member skill 技能
     * @return string
     */
    public static function person_skill(){
        return "temp";
    }

    /**
     * 职位的技能数量
     * @structure zset 有序集合
     * @member job_id 职位ID
     * @score  skill_count 技能数量
     * @return string
     */
    public static function job_count(){
        return "idx:jobs:req";
    }

    /**
     * 技能表(按技能索引职位)
     * @param $skill
     * @structure set 集合
     * @member job_id 职位ID
     * @return string
     */
    public static function skill($skill){
        return sprintf("idx:skill:%s",$skill);
    }

    /**
     * 全部职位
     * @structure set 集合
     * @member job_id 职位ID
     * @return string
     */
    public static function job_all(){
        return "job:all";
    }

    /**
     * 技能使用时长(年)
     * @param string $skill 技能
     * @structure zset 有序集合
     * @member job_id 职位ID
     * @score  year 使用时长
     * @return string
     */
    public static function skill_year($skill){
        return sprintf("skill:%s:year",$skill);
    }

    /**
     * 技能熟练度
     * @param string | int $skill 技能
     * @structure set 集合
     * @member job_id 职位ID
     * @return string
     */
    public static function skill_depth($skill,$depth){
        return sprintf("skill:%s:%s",$skill,$depth);
    }
}

/**
 * 职位业务类
 */
class JobClass {

    //redis-cli
    private $conn;
    //class Common
    private $commonClass;

    public function __construct(string $mode = Common::MODE_PURE)
    {
        $this->conn = RedisClient::getConn();
        $this->commonClass = new Common($mode);
    }

    /**
     * 设置技能
     * @warning 技能会被覆盖
     * @param int $job_id
     * @param array $skills
     * @return int
     */
    public function add_job(int $job_id,array $skills){
        return $this->conn->sadd(JobRedisKey::job($job_id),$skills);
    }

    /**
     * 是否符合职位要求
     * @param int $job_id 职位ID
     * @param array $skills 技能
     * @return bool
     */
    public function is_qualified(int $job_id,array $skills)
    {
        $pipe = $this->conn->pipeline();
        $pipe->del(JobRedisKey::person_skill());
        //生成个人技能
        $pipe->sadd(JobRedisKey::person_skill(),$skills);
        $pipe->expire(JobRedisKey::person_skill(),30);
        //是否符合技能要求
        $pipe->sdiff([JobRedisKey::job($job_id),JobRedisKey::person_skill()]);
        list($diff) = array_slice($pipe->execute(),-1,1);
        return !$diff;
    }

    /**
     * 创建技能索引
     * @param int $job_id 职位ID
     * @param array $skills 技能
     */
    public function index_job(int $job_id,array $skills){

        //技能索引
        $pipe = $this->conn->pipeline();
        foreach($skills as $skill){
            $pipe->sadd(JobRedisKey::skill($skill),[$job_id]);
        }
        //技能数量
        $pipe->zadd(JobRedisKey::job_count(),[$job_id=>count($skills)]);
        $pipe->execute();
    }

    /**
     * 匹配职位
     * @param array $skills 技能
     * @return mixed
     */
    public function find_jobs(array $skills){

        //查找skill表
        $skill_tables = array_map(function($skill){
            return JobRedisKey::skill($skill);
        },$skills);
        //匹配职位
        $pipe = $this->conn->pipeline();
        $find_jobs = $this->commonClass->zunionstore($pipe,$skill_tables,[],Common::DEFAULT_TTL,Common::EXECUTE_FALSE);
        $matched_skill_count = $this->commonClass->zinterstore($pipe,[$find_jobs,JobRedisKey::job_count()],["weights"=>[-1,1]],Common::DEFAULT_TTL,Common::EXECUTE_FALSE);
        $pipe->zrangebyscore($matched_skill_count,0,0);
        list($matched_jobs) = array_slice($pipe->execute(),-1,1);
        return $matched_jobs;
    }

    /**
     * 按技能使用时长(年)设置职位
     * @param int $job_id
     * @param array $skills_year
     */
    public function index_job_with_year(int $job_id,array $skills_year){
        $pipe = $this->conn->pipeline();
        //技能使用时长(年)表
        foreach($skills_year as $skill=>$year){
            $pipe->zadd(JobRedisKey::skill_year($skill),[$job_id=>$year]);
        }
        //全部职位表
        $pipe->sadd(JobRedisKey::job_all(),[$job_id]);
        //全部职位+技能数表
        $pipe->zadd(JobRedisKey::job_count(),[$job_id=>count($skills_year)]);
        $pipe->execute();
    }

    /**
     * 按技能使用时长(年)匹配职位
     * @param array $skills_year 技能与年限
     * @return mixed
     */
    public function find_jobs_with_year(array $skills_year){

        $pipe = $this->conn->pipeline();
        //获取指定技能使用时长(年)的job_id
        $find_jobs = [];
        foreach($skills_year as $skill=>$year){
            $jobs = $this->commonClass->zunionstore($pipe,[JobRedisKey::job_all(),JobRedisKey::skill_year($skill)],["weights"=>[-$year,1]],Common::DEFAULT_TTL,Common::EXECUTE_FALSE);
            $pipe->zremrangebyscore($jobs,'(0','inf');
            $find_jobs[] = $this->commonClass->zinterstore($pipe,[JobRedisKey::job_all(),$jobs],["weights"=>[1,0]],Common::DEFAULT_TTL,Common::EXECUTE_FALSE);
        }
        //计算职位技能数
        $jobs_count = $this->commonClass->zunionstore($pipe,$find_jobs,[],Common::DEFAULT_TTL,Common::EXECUTE_FALSE);
        //匹配技能数相同的职位
        $matched_jobs_count = $this->commonClass->zinterstore($pipe,[JobRedisKey::job_count(),$jobs_count],["weights"=>[-1,1]],Common::DEFAULT_TTL,Common::EXECUTE_FALSE);
        $pipe->zrangebyscore($matched_jobs_count,0,0);
        list($matched_jobs) = array_slice($pipe->execute(),-1,1);
        return $matched_jobs;
    }

    /**
     * 按技能熟练度设置职位
     * @param int $job_id 职位ID
     * @param array $skills_depth 技能+熟练度
     */
    public function index_job_with_depth(int $job_id,array $skills_depth){

        $pipe = $this->conn->pipeline();
        foreach($skills_depth as $skill => $depth){
            $pipe->sadd(JobRedisKey::skill_depth($skill,$depth),[$job_id]);
        }
        $pipe->zadd(JobRedisKey::job_count(),[$job_id=>count($skills_depth)]);
        $pipe->execute();
    }

    /**
     * 按技能熟练度匹配职位
     * @param array $skills_depth 技能+熟练度
     * @return mixed
     */
    public function find_jobs_with_depth(array $skills_depth){
        $pipe = $this->conn->pipeline();
        $find_job = [];
        foreach($skills_depth as $skill => $depth){
            $find_job[] = JobRedisKey::skill_depth($skill,$depth);
        }
        $jobs_count = $this->commonClass->zunionstore($pipe,$find_job,[],Common::DEFAULT_TTL,Common::EXECUTE_FALSE);
        $matched_jobs_count = $this->commonClass->zinterstore($pipe,[JobRedisKey::job_count(),$jobs_count],["weights"=>[-1,1]],Common::DEFAULT_TTL,Common::EXECUTE_FALSE);
        $pipe->zrangebyscore($matched_jobs_count,0,0);
        list($matched_jobs) = array_slice($pipe->execute(),-1,1);
        return $matched_jobs;
    }
}

/**
 * 控制台,debug类
 */
class JobConsole {

    //职位
    const JOB_FE = 1; //前端研发工程师
    const JOB_BE = 2; //后端研发工程师
    const JOB_DATA_ANALYST = 3;//数据分析师
    const JOB_UI = 3; //用户界面设计师

    //技能
    const SKILL_VUE = "vue";
    const SKILL_CSS = "css";
    const SKILL_JS = "js";
    const SKILL_PHP = "php";
    const SKILL_SQL = "sql";
    const SKILL_NGINX = "nginx";
    const SKILL_SPARK = "Spark";
    const SKILL_HIVE = "Hive";
    const SKILL_HBASE = "Hbase";
    const SKILL_PS = "ps";
    const SKILL_FLASH = "flash";

    //技能使用时长(年)
    const YEAR_ONE = 1;
    const YEAR_TWO = 2;
    const YEAR_THREE = 3;

    //技能熟练度
    const DEPTH_LOW = 1;
    const DEPTH_MID = 2;
    const DEPTH_HIGH = 3;

    const JOB_SKILL = [
        self::JOB_FE => [
            self::SKILL_VUE,
            self::SKILL_CSS,
            self::SKILL_JS,
        ],
        self::JOB_BE => [
            self::SKILL_PHP,
            self::SKILL_SQL,
            self::SKILL_NGINX,
        ]
    ];

    const JOB_SKILL_YEAR = [
        self::JOB_FE => [
            self::SKILL_VUE => self::YEAR_TWO,
            self::SKILL_CSS => self::YEAR_ONE,
            self::SKILL_JS  => self::YEAR_TWO,
        ],
        self::JOB_BE => [
            self::SKILL_PHP => self::YEAR_THREE,
            self::SKILL_SQL => self::YEAR_TWO,
            self::SKILL_NGINX => self::YEAR_TWO,
        ]
    ];

    const JOB_SKILL_DEPTH = [
        self::JOB_FE => [
            self::SKILL_VUE => self::DEPTH_LOW,
            self::SKILL_CSS => self::DEPTH_MID,
            self::SKILL_JS  => self::DEPTH_LOW,
        ],
        self::JOB_BE => [
            self::SKILL_PHP => self::DEPTH_HIGH,
            self::SKILL_SQL => self::DEPTH_HIGH,
            self::SKILL_NGINX => self::DEPTH_LOW,
        ]
    ];

    //方法1 匹配职位
    public static function run1(){
        $jobClass = new JobClass();
        //设置职位
        $jobClass->add_job(self::JOB_FE,self::JOB_SKILL[self::JOB_FE]);
        //匹配职位
        $matched1 = $jobClass->is_qualified(self::JOB_FE, self::JOB_SKILL[self::JOB_BE]);
        $matched2 = $jobClass->is_qualified(self::JOB_FE,self::JOB_SKILL[self::JOB_FE]);

        echo "matched1结果 [" . ($matched1 ? "成功" : "失败") . "]" . PHP_EOL;
        echo "matched2结果 [" . ($matched2 ? "成功" : "失败") . "]" . PHP_EOL;
    }

    //方法2 匹配职位
    public static function run2(){
        $jobClass = new JobClass();
        //设置技能索引
        $jobClass->index_job(self::JOB_BE,self::JOB_SKILL[self::JOB_BE]);
        //匹配职位
        $jobs = $jobClass->find_jobs(self::JOB_SKILL[self::JOB_BE]);
        if($jobs){
            echo "找到如下职位ID:" . PHP_EOL;
            echo implode(",",$jobs) . PHP_EOL;
        }else{
            echo "未找到职位" . PHP_EOL;
        }
    }

    //方法3 按技能使用时长(年)匹配职位
    public static function run3(){
        $jobClass = new JobClass();
        //设置技能索引
        $jobClass->index_job_with_year(self::JOB_BE,self::JOB_SKILL_YEAR[self::JOB_BE]);
        //匹配职位
        $jobs = $jobClass->find_jobs_with_year(self::JOB_SKILL_YEAR[self::JOB_BE]);
        if($jobs){
            echo "找到如下职位ID:" . PHP_EOL;
            echo implode(",",$jobs) . PHP_EOL;
        }else{
            echo "未找到职位" . PHP_EOL;
        }
    }

    //方法4 按技能熟练度匹配职位
    public static function run4(){
        $jobClass = new JobClass();
        //设置技能索引
        $jobClass->index_job_with_depth(self::JOB_BE,self::JOB_SKILL_DEPTH[self::JOB_BE]);
        //匹配职位
        $jobs = $jobClass->find_jobs_with_depth(self::JOB_SKILL_DEPTH[self::JOB_BE]);
        if($jobs){
            echo "找到如下职位ID:" . PHP_EOL;
            echo implode(",",$jobs) . PHP_EOL;
        }else{
            echo "未找到职位" . PHP_EOL;
        }
    }
}

//JobConsole::run1();

//JobConsole::run2();

//JobConsole::run3();

//JobConsole::run4();
