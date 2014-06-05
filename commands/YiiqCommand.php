<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains Yiiq main command class.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package ext.yiiq.commands
 */

/**
 * Yiiq main command class.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class YiiqCommand extends YiiqBaseCommand
{
    /**
     * Run worker for given queue.
     * 
     * @param  array[optional] $queue  Yiiq::DEFAULT_QUEUE by default
     * @param  int[optional] $threads   Yiiq::DEFAULT_THREADS by default
     */
    public function actionStart(array $queue = null, $threads = null)
    {
        Yii::app()->getComponent('yiiq');

        // Create array of unique queue names.
        // Split each --queue value by comma and then combine all values
        // into single-dimension array.
        $queues = $queue ?: array(Yiiq::DEFAULT_QUEUE);
        foreach ($queues as $index => $queue) {
            $queues[$index] = preg_split('/\s*\,\s*/', $queue);
        }
        $queues = call_user_func_array('array_merge', array_merge($queues));
        $queues = array_unique($queues);
        $stringifiedQueues = implode(', ', $queues);

        $threads = (int)$threads ?: Yiiq::DEFAULT_THREADS;

        $command = 'nohup sh -c "'.escapeshellarg(Yii::app()->basePath.'/yiic').' yiiqWorker run ';
        foreach ($queues as $queue) {
            $command .= '--queue='.$queue.' ';
        }
        $command .= '--threads='.$threads.'" > /dev/null 2>&1 &';
        $return = null;
        echo "Starting worker for $stringifiedQueues ($threads threads)... ";
        exec($command, $return);
        echo "Done.\n";
    }

    /**
     * Stop all active workers.
     */
    public function actionStop()
    {
        Yii::app()->getComponent('yiiq')->check();

        $pids = Yii::app()->yiiq->pidPool->getData();
        if ($pids) {
            foreach ($pids as $pid) {
                echo "Killing $pid... ";
                posix_kill($pid, SIGTERM);
                while (Yii::app()->yiiq->isPidAlive($pid)) {
                    usleep(500000);
                }
                echo "Done.\n";
            }
        }
        else {
            echo "No pids found.\n";
        }
    }

    /**
     * Check redis db consistency.
     */
    public function actionCheck()
    {
        Yii::app()->getComponent('yiiq')->check();
    }
    
}