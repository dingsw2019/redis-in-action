<?php

/**
 * //测试 用于消费执行队列
 */

require_once ("./queue.php");
require_once("../ch08/social.php");

$queue_name = "default";
$callbacks = new Social();

$queue = new Queue();
$queue->work_watch_queue($queue_name,$callbacks);