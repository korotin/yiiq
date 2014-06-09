<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains Yiiq worker command class.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package ext.yiiq.commands
 */

/**
 * Yiiq worker command class.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class YiiqWorkerCommand extends YiiqBaseCommand
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
     * @var ARedisSet
     */
    protected $childPool;
    /**
     * Shutdown flag.
     * Becomes true on SIGTERM.
     * 
     * @var boolean
     */
    protected $shutdown = false;

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
        $titleTemplate = Yii::app()->yiiq->titleTemplate;
        if (!$titleTemplate) return;

        $placeholders = array(
            '{name}'    => $this->instanceName,
            '{type}'    => $this->processType,
            '{queue}'   => $queue ?: $this->stringifiedQueues,
            '{message}' => $title,
        );

        $title = str_replace(array_keys($placeholders), array_values($placeholders), $titleTemplate);
        if (function_exists('cli_set_process_title')) {
            cli_set_process_title($title);
        }
        elseif (function_exists('setproctitle')) {
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
        return Yii::app()->yiiq->prefix.':workers:'.$queue;
    }

    /**
     * Get child pid pool.
     * 
     * @return ARedisSet
     */
    protected function getChildPool()
    {
        if ($this->childPool === null) {
            $this->childPool = new ARedisSet(Yii::app()->yiiq->prefix.':children:'.$this->pid);
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
                ($oldPid = Yii::app()->redis->get($this->getWorkerPidName($queue)))
                && (Yii::app()->yiiq->isPidAlive($oldPid))
            ) {
                Yii::trace('Worker for queue '.$queue.' already running.');
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
            Yii::app()->redis->set($this->getWorkerPidName($queue), $this->pid);
        }

        Yii::app()->yiiq->pidPool->add($this->pid);
    }

    /**
     * Remove current pid from redis.
     */
    protected function clearPid()
    {
        foreach ($this->queues as $queue) {
            Yii::app()->redis->del($this->getWorkerPidName($queue));
        }

        Yii::app()->yiiq->pidPool->remove($this->pid);
    }

    /**
     * Set up handlers for signals.
     */
    protected function setupSignals()
    {
        declare(ticks = 1);
        // Set shutdown flag on terminate command
        pcntl_signal(SIGTERM, function() {
            $this->shutdown = true;
        });
        // Grab child process status to prevent appearing of zombie processes
        pcntl_signal(SIGCHLD, function() {
            $status = null;
            if (pcntl_waitpid(-1, $status, WNOHANG) > 0) {
                pcntl_wexitstatus($status);
            }
        });
    }

    /**
     * Get active child threads count.
     * 
     * @return int
     */
    protected function getThreadsCount()
    {
        return (int)Yii::app()->redis->getClient()->scard($this->getChildPool()->name);
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
     * Wait for child threads to complete.
     */
    protected function waitForThreads()
    {
        $status = null;
        while (pcntl_waitpid(-1, $status) > 0) { }
    }

    /**
     * Run job with given id.
     * Job will be executed in fork and method will return 
     * after the fork get initialized.
     *
     * @param  string $queue
     * @param  string $jobId
     */
    protected function runJob($queue, $job)
    {
        // If we're in parent process, wait for child thread to get initialized
        // and then return
        if (pcntl_fork() !== 0) {
            usleep(100000);
            return;
        }

        $childPid = posix_getpid();
        
        $this->processType = 'job';
        $this->setProcessTitle('initializing', $queue);

        // Force reconnect to redis
        Yii::app()->redis->setClient(null);

        $this->getChildPool()->add($childPid);

        extract($job);
        Yii::trace('Starting job '.$queue.':'.$id.' ('.$class.')...');
        $this->setProcessTitle('executing '.$id.' ('.$class.')', $queue);

        $e = null;
        try {
            $job = new $class($queue, $type, $id);
            $job->execute($args);
        }
        catch (Exception $e) {
            // Exception is caught, but we'll try to exit child thread correctly
        }

        $this->getChildPool()->remove($childPid);

        if (!$e) {
            Yii::trace('Job '.$queue.':'.$id.' done.');
        }
        else {
            throw $e;
        }

        exit(0);
    }

    /**
     * Main loop.
     */
    protected function loop()
    {
        $status = null;
        $offset = null;
        $count = count($this->queues);

        while (true) {
            // Iterate over free threads.
            while (
                !$this->shutdown
                && $this->hasFreeThread()
            ) {
                // Look for new job.
                // Iterate over all watched queues, stop when new job found.
                // If the previous job was found in first queue, iteration will be started from second
                // queue.
                for ($index = 0; $index < $count; $index++) {
                    $queue = $this->queues[($index + $offset) % $count];
                    if ($job = Yii::app()->yiiq->popJob($queue)) {
                        $offset = ($index + 1) % $count;
                        break;
                    }
                }
                
                // No job was found - exit loop.
                if (!$job) break;

                // Execute found job.
                $this->runJob($queue, $job);
            }

            if ($this->shutdown) break;

            // If all child threads are active, wait for one of them to terminate.
            // Otherwise just sleep for 1 second.
            if (!$this->hasFreeThread()) {
                $this->setProcessTitle('no free threads ('.$this->maxThreads.' threads)');
                pcntl_waitpid(-1, $status);
            }
            else {
                $this->setProcessTitle('no new jobs ('.$this->getThreadsCount().' of '.$this->maxThreads.' threads busy)');
                sleep(1);
            }

            // Check for dead pids in child pool
            Yii::app()->yiiq->checkPidPool($this->getChildPool());
        }
    }
    
    /**
     * Run worker for given queue and with given count of
     * max child threads.
     * 
     * @param  array $queues 
     * @param  int $threads
     */
    public function actionRun(array $queue, $threads)
    {
        $this->instanceName = Yii::app()->yiiq->name ?: Yii::getPathOfAlias('application');
        $this->pid          = posix_getpid();
        $this->queues       = $queue;
        $this->stringifiedQueues = $this->stringifyQueues($this->queues);
        $this->maxThreads   = (int) $threads;
        
        $this->setProcessTitle('initializing');

        $this->checkRunningWorkers();
        $this->setupSignals();
        $this->savePid();

        Yii::trace('Started new yiiq worker '.$this->pid.' for '.$this->stringifiedQueues.'.');

        $this->loop();
       
        $this->setProcessTitle('terminating');
        
        $this->waitForThreads();
        $this->clearPid();
        
        Yii::trace('Terminated yiiq worker '.$this->pid.'.');
    }

}