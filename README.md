### 学习总结

1.set与zset可执行zunionstore,set的score为1

```
>>> smembers job
php
java
python

>>> zrange job:weight 0 -1 withscores
js     1
php    2
java   3
python 4

>>> zunionstore union:job:weight 2 job job:weight
>>> zrange union:job:weight 0 -1 withscores
js     1
php    3
java   4
python 5
```

2.匹配有序集合的某个score，不能用zrangebyscore
```
>>> smembers job
job1
job2
job3
job4

>>> zrange skill:year 0 -1 withscores
job1 1
job2 2
job3 3
job4 2

#查找2年工作经验的职位
>>> zunionstore union:skill:year 2 job skill:year weights -2 1
>>> zrangebyscore union:skill:year 0 0
job2
job4
```

3.范围匹配
```
//-1表示最后一个值,并且包含这个值返回
>>> zrange skill:year 0 -1 withscores
job1 1
job2 2
job3 3
job4 2
job5 4
job6 5

# 匹配工作经验大于等于3年的
>>> zrangebyscore skill:year 3 inf withscores
job3 3
job5 4
job6 5

# 匹配工作经验大于3年的
>>> zrangebyscore skill:year (3 inf withscores
job5 4
job6 5

```

4.predis的watch
```
$trans = $conn->transaction(['cas'=>true]);
try{
    $trans->watch($key);
    if($trans->get($key) == $value){
        $trans->multi();
        $trans->del($key);
        $trans->exec();
    }
}catch (\Predis\Transaction\AbortedMultiExecException $e){
    echo $e->getMessage();
}

//特别注意,以下方式的watch无效,还不知道为什么,todo 待看源码
$conn->transaction(['watch'=>$key,'cas'=>true]);
```

5.复合排序
- 论坛文章基于更新时间和投票数量做倒叙排列
```
//更新时间的有序集合
zadd update doc_id update_time
//投票数的有序集合
zadd vote doc_id vote_count
//将搜索到的文章与更新时间和投票数做交集,分数相加
zinterstore update_vote_sort 3 search_docs_id update vote weight 0 1 1
//倒排分数
zrevrange update_vote_sort 0 -1
```

常见报错集合
```
1.watch监控的key被修改,报如下错误
The current transaction has been aborted by the server
当前事务已被服务器中止
```