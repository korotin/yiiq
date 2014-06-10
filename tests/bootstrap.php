<?php
$yiit='yii/yiit.php';
$config=__DIR__.'/config/test.php';

require_once $yiit;
Yii::createConsoleApplication($config);