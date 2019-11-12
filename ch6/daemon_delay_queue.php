<?php
/**
 * 测试, 用于消费延迟队列
 */

require_once ("./queue.php");
$queue = new Queue();
$queue->poll_queue();