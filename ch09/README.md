### 1.短结构
- 应用场景：大数据集,比如所有用户的地理位置存一个key中
- 适用结构：集合、列表、散列
- 实现方式：根据需求计算单键所需空间，修改ziplist最大限制，使每个键都使用ziplist结构
```
list-max-ziplist-entries 512    # 列表结构使用压缩列表表示的限制条件。
list-max-ziplist-value 64       #
hash-max-ziplist-entries 512    # 散列结构使用压缩列表表示的限制条件
hash-max-ziplist-value 64       #（Redis 2.6 以前的版本会为散列结构使用不同的编码表示，并且选项的名字也与此不同）。
zset-max-ziplist-entries 128    # 有序集合使用压缩列表表示的限制条件。
zset-max-ziplist-value 64       #
```

### 2.分片(类似数据库分表)
- 大数据集,比如所有用户的地理位置存一个key中
- 适用结构：集合、列表、散列
- 实现方式：将大数据集根据某唯一值，切分成n块，每一块不要超过ziplist最大限制。通过ziplist达到空间小,读写快的效果


### 3.打包存储二进制位和字节
- 应用场景：存储简短且长度固定的连续ID
- 实现方式：根据ID得分片ID和偏移量,然后setrange写入字符串,内容是固定的很重要！
```
<?php
//将所有用户性别按分片方式存入string结构
$uid = 1001;
$sex = M;
$shard_id = intval($uid / 1000);
$offset = $uid % 1000;
$rediscli->setrange(sprintf('user:sex:%s',$shard_id),$offset,$sex);
```