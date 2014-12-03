<?php
$yiit=__DIR__.'/../vendor/yiisoft/yii/framework/yiit.php';
$config=__DIR__.'/config/test.php';

require_once $yiit;
Yii::createConsoleApplication($config);
