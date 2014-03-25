<?php

/**
 * @see http://www.yiiframework.com/forum/index.php/topic/10421-logging-in-long-running-console-app/
 */
class YiiqBaseCommand extends CConsoleCommand
{
    public function run($args)
    {
        Yii::getLogger()->autoFlush = 1;
        Yii::getLogger()->detachEventHandler('onFlush',array(Yii::app()->log,'collectLogs'));
        Yii::getLogger()->attachEventHandler('onFlush',array($this,'processLogs'));
        parent::run($args);
    }

    public function processLogs($event)
    {
        static $routes;
        $logger = Yii::getLogger();
        $routes = isset($routes) ? $routes : Yii::app()->log->getRoutes();
        foreach($routes as $route)
        {
            if($route->enabled)
            {
                $route->collectLogs($logger,true);
                $route->logs = array();
            }
        }
    }
}