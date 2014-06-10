<?php

require_once __DIR__.'/../../../../../vendors/autoload.php';

return array(
    'basePath' => __DIR__.'/..',
    'extensionPath' => __DIR__.'/../..',

    'aliases' => array(
        'vendors' => __DIR__.'/../../../../../vendors',
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
            'class' => 'vendors.subtronic.yiipredis.ARedisConnection',
            'hostname' => 'localhost',
            'port' => 6379,
            'database' => 2,
            'prefix' => 'yiiqtest:',
        ),

        'yiiq' => array(
            'class' => 'ext.components.Yiiq',
            'name' => 'YiiqTest',
        ),
    ),

    'commandMap' => array(
        'yiiq' => array(
            'class' => 'vendors.herroffizier.yiiq.commands.YiiqCommand',
        ),
        'yiiqWorker' => array(
            'class' => 'vendors.herroffizier.yiiq.commands.YiiqWorkerCommand',
        ),
    ),
);