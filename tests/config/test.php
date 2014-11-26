<?php

$token = getenv('TEST_TOKEN');
if (strlen($token) !== 32) {
    $token = md5($token.'_'.microtime(true));
}
define('TEST_TOKEN',  $token);
unset($token);

require_once __DIR__.'/../../vendor/autoload.php';

return array(
    'basePath' => __DIR__.'/..',
    'extensionPath' => __DIR__.'/../..',

    'aliases' => array(
        'Yiiq' => __DIR__.'/../../src',
        'Yiiq\tests' => __DIR__.'/../',
        'vendor' => __DIR__.'/../../vendor',
    ),

    'preload' => array(
        'log',
    ),

    'components' => array(
        'log'=>array(
            'class'=>'CLogRouter',
        ),

        'redis' => array(
            'class' => 'vendor.codemix.yiiredis.ARedisConnection',
            'hostname' => 'localhost',
            'port' => 6379,
            'database' => 2,
            'prefix' => 'yiiqtest:'.TEST_TOKEN.':',
        ),

        'yiiq' => array(
            'class' => '\Yiiq\Yiiq',
            'name' => 'YiiqTest_'.TEST_TOKEN,
            'faultIntervals' => [1, 1, 1],
        ),
    ),

    'commandMap' => array(
        'yiiq' => array(
            'class' => '\Yiiq\commands\Main',
        ),
        'yiiqWorker' => array(
            'class' => '\Yiiq\commands\Worker',
        ),
    ),
);