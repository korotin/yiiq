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
     * @param  string[optional] $queue  Yiiq::DEFAULT_QUEUE by default
     * @param  int[optional] $threads   Yiiq::DEFAULT_THREADS by default
     */
    public function actionStart($queue = null, $threads = null)
    {
        Yii::app()->getComponent('yiiq');

        $queue = $queue ?: Yiiq::DEFAULT_QUEUE;
        $threads = (int)$threads ?: Yiiq::DEFAULT_THREADS;

        $command = 'nohup sh -c "'.escapeshellarg(Yii::app()->basePath.'/yiic').' yiiqWorker run --queue='.$queue.' --threads='.$threads.'" > /dev/null 2>&1 &';
        $return = null;
        echo "Starting worker for $queue ($threads threads)... ";
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