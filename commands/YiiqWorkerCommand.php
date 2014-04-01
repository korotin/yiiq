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
     * Worker pid.
     * 
     * @var int
     */
    protected $pid;
    /**
     * Worker queue name.
     * 
     * @var string
     */
    protected $queue;
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
     * Run job with given id.
     * Job will be executed in fork and method will return 
     * after the fork get initialized.
     * 
     * @param  string $jobId
     */
    protected function runJob($job)
    {
        // If we're in parent process, wait for child thread to get initialized
        // and then return
        if (pcntl_fork() !== 0) {
            usleep(100000);
            return;
        }

        $childPid = posix_getpid();

        // Force reconnect to redis
        Yii::app()->redis->setClient(null);

        $this->getChildPool()->add($childPid);

        Yii::trace('Forked process for job '.$job['id'].'.');

        extract($job);
        Yii::trace('Starting job '.$this->queue.':'.$id.' ('.$class.')...');

        $e = null;
        try {
            $job = new $class($this->queue, $type, $id);
            $job->execute($args);
        }
        catch (CException $e) {

        }

        $this->getChildPool()->remove($childPid);

        if (!$e) {
            Yii::trace('Job '.$this->queue.':'.$id.' done.');
        }
        else {
            throw $e;
        }

        exit(0);
    }
    
    /**
     * Run worker for given queue and with given count of
     * max child threads.
     * 
     * @param  string $queue 
     * @param  int $threads
     */
    public function actionRun($queue, $threads)
    {
        $this->pid          = posix_getpid();
        $this->queue        = $queue;
        $this->maxThreads   = (int) $threads;

        $this->setupSignals();

        Yii::app()->yiiq->pidPool->add($this->pid);

        Yii::trace('Started new yiiq worker '.$this->pid.' for queue '.($this->queue ?: Yiiq::DEFALUT_QUEUE).'.');

        $status = null;
        while (true) {
            while (
                !$this->shutdown
                && $this->hasFreeThread()
            ) {
                if (!($job = Yii::app()->yiiq->popJob($this->queue))) break;
                $this->runJob($job);
            }

            if ($this->shutdown) break;

            Yii::app()->yiiq->checkPidPool($this->getChildPool());

            if ($this->getThreadsCount()) {
                pcntl_waitpid(-1, $status);
            }
            else {
                sleep(1);
            }
        }

        Yii::trace('Waiting for child threads to terminate...');
        while (pcntl_waitpid(-1, $status) > 0) {
            Yii::trace($this->getThreadsCount().' threads left.');
        }

        Yii::app()->yiiq->pidPool->remove($this->pid);
        Yii::trace('Terminated yiiq worker '.$this->pid.'.');
    }

}