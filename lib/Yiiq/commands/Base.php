<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains base Yiiq command class.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.commands
 */

namespace Yiiq\commands;

/**
 * Yiiq base command class.
 *
 * It processes every log message to available log routes.
 * 
 * @see http://www.yiiframework.com/forum/index.php/topic/10421-logging-in-long-running-console-app/
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class Base extends \CConsoleCommand
{
    public function run($args)
    {
        \Yii::getLogger()->autoFlush = 1;
        \Yii::getLogger()->detachEventHandler('onFlush',[\Yii::app()->log,'collectLogs']);
        \Yii::getLogger()->attachEventHandler('onFlush',[$this,'processLogs']);
        parent::run($args);
    }

    public function processLogs($event)
    {
        static $routes;
        $logger = \Yii::getLogger();
        $routes = isset($routes) ? $routes : \Yii::app()->log->getRoutes();
        foreach($routes as $route)
        {
            if($route->enabled)
            {
                $route->collectLogs($logger,true);
                $route->logs = [];
            }
        }
    }
}