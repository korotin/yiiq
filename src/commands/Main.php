<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains Yiiq main command class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.commands
 */

namespace Yiiq\commands;

use Yiiq\Yiiq;

/**
 * Yiiq main command class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class Main extends Base
{
    /**
     * Run worker for given queue.
     *
     * @param string[]|null $queue   \Yiiq\Yiiq::DEFAULT_QUEUE by default
     * @param intteger|null $threads \Yiiq\Yiiq::DEFAULT_THREADS by default
     * @param string|null   $log     error log file name stored at application.runtime
     */
    public function actionStart(array $queue = null, $threads = null, $log = null)
    {
        \Yii::app()->getComponent('yiiq');

        // Create array of unique queue names.
        // Split each --queue value by comma and then combine all values
        // into single-dimension array.
        $queues = $queue ?: [Yiiq::DEFAULT_QUEUE];
        foreach ($queues as $index => $queue) {
            $queues[$index] = preg_split('/\s*\,\s*/', $queue);
        }
        $queues = call_user_func_array('array_merge', array_merge($queues));
        $queues = array_unique($queues);
        $stringifiedQueues = implode(', ', $queues);

        // Set threads value.
        $threads = abs((int) $threads) ?: Yiiq::DEFAULT_THREADS;

        // Set log file name if $log is not empty, otherwise disable error logging.
        $log =
            $log
                ? \Yii::getPathOfAlias('application.runtime').DIRECTORY_SEPARATOR.$log
                : '/dev/null';

        // Used for testing with paratest.
        if (defined('TEST_TOKEN')) {
            $vars = 'TEST_TOKEN=\''.TEST_TOKEN.'\' ';
        } else {
            $vars = '';
        }

        $command = 'nohup sh -c "'.$vars.escapeshellarg(\Yii::app()->basePath.'/yiic').' yiiqWorker run ';
        foreach ($queues as $queue) {
            $command .= '--queue='.$queue.' ';
        }
        $command .= '--threads='.$threads.'" > '.escapeshellarg($log).' 2>&1 &';
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
        \Yii::app()->yiiq->health->check();
        $pids = \Yii::app()->yiiq->pools->pids->getData();
        if ($pids) {
            foreach ($pids as $pid) {
                echo "Killing $pid... ";
                posix_kill($pid, SIGTERM);
                while (\Yii::app()->yiiq->health->isPidAlive($pid)) {
                    usleep(100000);
                }
                echo "Done.\n";
            }
        } else {
            echo "No pids found.\n";
        }
    }

    /**
     * Get worker statuses.
     */
    public function actionStatus()
    {
        $status = 0;
        $pids = \Yii::app()->yiiq->pools->pids->getData();
        if ($pids) {
            $dead = [];
            $alive = [];
            foreach ($pids as $pid) {
                $isAlive = \Yii::app()->yiiq->health->isPidAlive($pid);
                if ($isAlive) {
                    $alive[] = $pid;
                } else {
                    $dead[] = $pid;
                }
            }

            if ($dead) {
                if ($alive) {
                    echo "Some dead processes (".implode(', ', $dead).") found! "
                        ."Run './yiic yiiq check' to remove them.\n";
                } else {
                    echo "All processes (".implode(', ', $dead).") are dead. System is not working.\n";
                }
                $status = 1;
            } else {
                echo "All processes (".implode(', ', $alive).") are alive. Everything looks good.\n";
            }
        } else {
            echo "No processes found. System is not working.\n";
            $status = 1;
        }

        exit($status);
    }

    /**
     * Check system health and fix problems.
     */
    public function actionCheck()
    {
        \Yii::app()->yiiq->health->check();
    }
}
