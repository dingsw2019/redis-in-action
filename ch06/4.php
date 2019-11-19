<?php

require_once ("../RedisClient.php");

$conn = RedisClient::getConn();


function readblocks($key,$blocksize=2**17){
    global $conn;

    $lb = $blocksize;
    $pos = 0;
    while($lb == $blocksize){
        $block = $conn->getrange($key,$pos,$pos + $blocksize - 1);
        yield $block;

        $lb = strlen($block);
        $pos += $lb;
    }
    yield '';
}

foreach(readblocks('str') as $b){
    var_dump($b);
}
