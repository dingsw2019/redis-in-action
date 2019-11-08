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