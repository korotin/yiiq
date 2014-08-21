<?php

require_once __DIR__.'/../../../../autoload.php';

return array(
    'basePath' => __DIR__.'/..',
    'extensionPath' => __DIR__.'/../..',

    'aliases' => array(
        'vendor' => __DIR__.'/../../../../',
    ),

    'import' => array(
        'ext.commands.*',
        'ext.components.*',
        'ext.jobs.*',
        'ext.models.*',
        'ext.tests.helpers.*',
    ),

    'preload' => array(
        'log',
    ),

    'components' => array(
        'log'=>array(
            'class'=>'CLogRouter',
        ),

        'redis' => array(
            'class' => 'vendor.subtronic.yiipredis.ARedisConnection',
            'hostname' => 'localhost',
            'port' => 6379,
            'database' => 2,
            'prefix' => 'yiiqtest:',
        ),

        'yiiq' => array(
            'class' => 'ext.components.Yiiq',
            'name' => 'YiiqTest',
            'faultIntervals' => [1, 1, 1],
        ),
    ),

    'commandMap' => array(
        'yiiq' => array(
            'class' => 'vendor.herroffizier.yiiq.commands.YiiqCommand',
        ),
        'yiiqWorker' => array(
            'class' => 'vendor.herroffizier.yiiq.commands.YiiqWorkerCommand',
        ),
    ),
);