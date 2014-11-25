<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains Yiiq worker command class.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.commands
 */

namespace Yiiq\commands;

use Yiiq\Yiiq;
use Yiiq\commands\Base;
use Yiiq\jobs\Data;

/**
 * Yiiq worker command class.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class Worker extends Base
{
    /**
     * Instance name,
     * 
     * @var string
     */
    protected $instanceName = null;

    /**
     * Process type displayed in process title.
     * 
     * @var string
     */
    protected $processType = 'worker';

    /**
     * Worker pid.
     * 
     * @var int
     */
    protected $pid;

    /**
     * Worker queues.
     * 
     * @var array
     */
    protected $queues;

    /**
     * Stringifies queues names.
     * 
     * @var string
     */
    protected $stringifiedQueues;

    /**
     * Max count of child threads.
     * 
     * @var int
     */
    protected $maxThreads;

    /**
     * Child pid pool.
     * 
     * @var \ARedisSet
     */
    protected $childPool;

    /**
     * Child pid to job id array.
     * 
     * @var array
     */
    protected $pidToJob = [];

    /**
     * Shutdown flag.
     * Becomes true on SIGTERM.
     * 
     * @var boolean
     */
    protected $shutdown = false;

    protected $signalHandled = false;

    /**
     * Stringify queues array.
     * 
     * @param  array  $queues
     * @return string
     */
    protected function stringifyQueues(array $queues)
    {
        asort($queues);
        return implode(',', $queues);
    }

    /**
     * Set process title to given string.
     * Process title is changing by cli_set_process_title (PHP >= 5.5) or
     * setproctitle (if proctitle extension is available).
     * 
     * @param string $title
     * @param string[optional] $queue
     */
    protected function setProcessTitle($title, $queue = null)
    {
        $titleTemplate = \Yii::app()->yiiq->titleTemplate;
        if (!$titleTemplate) {
            return;
        }

        $placeholders = array(
            '{name}'    => $this->instanceName,
            '{type}'    => $this->processType,
            '{queue}'   => $queue ?: $this->stringifiedQueues,
            '{message}' => $title,
        );

        $title = str_replace(array_keys($placeholders), array_values($placeholders), $titleTemplate);
        if (function_exists('cli_set_process_title')) {
            cli_set_process_title($title);
        } elseif (function_exists('setproctitle')) {
            setproctitle($title);
        }
    }

    /**
     * Return queue-to-pid key name.
     *  
     * @return string
     */
    protected function getWorkerPidName($queue)
    {
        return \Yii::app()->yiiq->prefix.':workers:'.$queue;
    }

    /**
     * Get child pid pool.
     * 
     * @return ARedisSet
     */
    protected function getChildPool()
    {
        if ($this->childPool === null) {
            $this->childPool = new \ARedisSet(\Yii::app()->yiiq->prefix.':children:'.$this->pid);
        }

        return $this->childPool;
    }

    /**
     * Check if given queues are already watched.
     */
    protected function checkRunningWorkers()
    {
        foreach ($this->queues as $queue) {
            if (
                ($oldPid = \Yii::app()->redis->get($this->getWorkerPidName($queue)))
                && (\Yii::app()->yiiq->isPidAlive($oldPid))
            ) {
                \Yii::trace('Worker for queue '.$queue.' already running.');
                exit(1);
            }
        }
    }

    /**
     * Save current pid to redis.
     */
    protected function savePid()
    {
        foreach ($this->queues as $queue) {
            \Yii::app()->redis->set($this->getWorkerPidName($queue), $this->pid);
        }

        \Yii::app()->yiiq->pidPool->add($this->pid);
    }

    /**
     * Remove current pid from redis.
     */
    protected function clearPid()
    {
        foreach ($this->queues as $queue) {
            \Yii::app()->redis->del($this->getWorkerPidName($queue));
        }

        \Yii::app()->yiiq->pidPool->remove($this->pid);
    }

    /**
     * Handle SIGTERM.
     */
    protected function handleSigTerm()
    {
        $this->signalHandled = true;
        $this->shutdown = true;
    }

    /**
     * Handle SIGCHLD.
     */
    protected function handleSigChld()
    {
        $this->signalHandled = true;
        $this->waitForThread(WNOHANG | WUNTRACED);
    }

    protected function getSignalHandlers()
    {
        return [
            SIGTERM => 'handleSigTerm',
            SIGCHLD => 'handleSigChld',
        ];
    }

    /**
     * Set up handlers for signals.
     */
    protected function setupSignals()
    {
        foreach ($this->getSignalHandlers() as $signal => $method) {
            pcntl_signal($signal, [$this, $method]);
        }
    }

    /**
     * Wait some time for signal.
     * If signal received, it will be correclty handled.
     */
    protected function waitForSignals()
    {
        $handlers = $this->getSignalHandlers();

        $siginfo = [];
        $signal = pcntl_sigtimedwait(
            array_keys($handlers),
            $siginfo,
            0,
            pow(10, 9) * 0.01
        );
        
        if (isset($handlers[$signal])) {
            $this->{$handlers[$signal]}();
        }
    }

    /**
     * Dispatch all signals.
     */
    protected function dispatchSignals()
    {
        do {
            $this->signalHandled = false;
            pcntl_signal_dispatch();
        } while ($this->signalHandled);
    }

    /**
     * Get active child threads count.
     * 
     * @return int
     */
    protected function getThreadsCount()
    {
        return (int) \Yii::app()->redis->getClient()->scard($this->getChildPool()->name);
    }

    /**
     * Can we fork one more thread?
     * 
     * @return boolean
     */
    protected function hasFreeThread()
    {
        return $this->getThreadsCount() < $this->maxThreads;
    }

    /**
     * Wait for any child to exit.
     * If $options = WNOHANG returns immediately if no child process exited.
     * 
     * @param  integer[optional] $options
     */
    protected function waitForThread($options = 0)
    {
        // Receive all child pids.
        do {
            $status = null;
            $childPid = pcntl_wait($status, $options);
            
            if ($childPid <= 0) {
                return;
            }

            pcntl_wexitstatus($status);
            $this->getChildPool()->remove($childPid);

            if (!isset($this->pidToJob[$childPid])) {
                return;
            }

            $jobId = $this->pidToJob[$childPid];
            unset($this->pidToJob[$childPid]);

            // If status is non-zero or job is still marked as executing,
            // child process failed.
            if ($status || \Yii::app()->yiiq->isExecuting($jobId)) {
                \Yii::app()->yiiq->restoreJob($jobId);
            }
        } while ($childPid > 0);
    }

    /**
     * Wait for child threads to complete.
     */
    protected function waitForThreads()
    {
        while ($this->getThreadsCount()) {
            $this->waitForThread();
        }
    }

    /**
     * Run job with given id.
     * Job will be executed in fork and method will return 
     * after the fork get initialized.
     *
     * @param  string $queue
     * @param  \Yiiq\jobs\Data $job
     */
    protected function runJob($queue, Data $jobData)
    {
        // Try to fork process.
        $childPid = pcntl_fork();

        // Force reconnect to redis for parent and child due to bug in PhpRedis
        // (https://github.com/nicolasff/phpredis/issues/474).
        \Yii::app()->redis->getClient(true);
        
        if ($childPid > 0) {
            // If we're in parent process, add child pid to pool and return.
            $this->pidToJob[$childPid] = $jobData->id;
            $this->getChildPool()->add($childPid);
            return;
        } elseif ($childPid < 0) {
            // If we're failed to fork process, restore job and exit.
            \Yii::app()->restoreJob($jobData->id);
            return;
        }

        // We are child - get our pid.
        $childPid = posix_getpid();
        
        $this->processType = 'job';
        $this->setProcessTitle('initializing', $queue);

        \Yii::trace('Starting job '.$jobData->queue.':'.$jobData->id.' ('.$jobData->class.')...');
        $this->setProcessTitle('executing '.$jobData->id.' ('.$jobData->class.')', $jobData->queue);

        \Yii::app()->yiiq->markAsStarted($jobData, $childPid);
        \Yii::app()->yiiq->onBeforeJob();

        $class = $jobData->class;
        $job = new $class($jobData->queue, $jobData->type, $jobData->id);
        $returnData = $job->execute($jobData->args);

        \Yii::app()->yiiq->markAsCompleted($jobData, $returnData);
        \Yii::app()->yiiq->onAfterJob();
        
        \Yii::trace('Job '.$jobData->queue.':'.$jobData->id.' done.');

        exit(0);
    }

    /**
     * Main loop.
     */
    protected function loop()
    {
        $offset = null;
        $count  = count($this->queues);

        while (!$this->shutdown) {
            // Iterate over free threads.
            while ($this->hasFreeThread() && !$this->shutdown) {
                // Handle signals.
                $this->dispatchSignals();

                if ($this->shutdown) {
                    break;
                }

                // Look for new job.
                // Iterate over all watched queues, stop when new job found.
                // If the previous job was found in first queue, iteration will be started from second
                // queue etc.
                for ($index = 0; $index < $count; $index++) {
                    $queue = $this->queues[($index + $offset) % $count];
                    if ($jobData = \Yii::app()->yiiq->popJob($queue)) {
                        $offset = ($index + 1) % $count;
                        break;
                    }
                }
                
                // No job was found - exit loop.
                if (!$jobData) {
                    break;
                }

                // Execute found job.
                $this->runJob($queue, $jobData);
            }

            if ($this->shutdown) {
                break;
            }

            // If no free slots available wait for any child to exit. Otherwise just wait some time.
            if (!$this->hasFreeThread()) {
                $this->setProcessTitle(
                    'no free threads ('.$this->maxThreads.' threads)'
                );

                $this->waitForThread();
            } else {
                $this->setProcessTitle(
                    'no new jobs ('.$this->getThreadsCount()
                    .' of '.$this->maxThreads.' threads busy)'
                );
                
                // Wait a little before next loop.
                $this->waitForSignals();
            }

            // Handle signals.
            $this->dispatchSignals();
        }
    }
    
    /**
     * Run worker for given queues and with given count of
     * max child threads.
     * 
     * @param  array $queues 
     * @param  int $threads
     */
    public function actionRun(array $queue, $threads)
    {
        $this->instanceName = \Yii::app()->yiiq->name ?: \Yii::getPathOfAlias('application');
        $this->pid          = posix_getpid();
        $this->queues       = $queue;
        $this->stringifiedQueues = $this->stringifyQueues($this->queues);
        $this->maxThreads   = (int) $threads;
        
        $this->setProcessTitle('initializing');

        $this->checkRunningWorkers();
        $this->setupSignals();
        $this->savePid();

        \Yii::trace('Started new yiiq worker '.$this->pid.' for '.$this->stringifiedQueues.'.');

        $this->loop();
       
        $this->setProcessTitle('terminating');
        
        $this->waitForThreads();
        $this->clearPid();
        
        \Yii::trace('Terminated yiiq worker '.$this->pid.'.');
    }
}
