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
use Yiiq\jobs\Job;
use Yiiq\jobs\Runner;

/**
 * Yiiq worker command class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class Worker extends Base
{
    /**
     * Worker pid.
     *
     * @var int
     */
    protected $pid;

    /**
     * Worker queues.
     *
     * @var string[]
     */
    protected $queues;

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
     * @var Job[]
     */
    protected $pidToJob = [];

    /**
     * Shutdown flag.
     * Becomes true on SIGTERM.
     *
     * @var boolean
     */
    protected $shutdown = false;

    /**
     * Whether signal handled already.
     *
     * @var boolean
     */
    protected $signalHandled = false;

    /**
     * Return queue-to-pid key name.
     *
     * @param  string $queue
     * @return string
     */
    protected function getWorkerPidName($queue)
    {
        return \Yii::app()->yiiq->prefix.':workers:'.$queue;
    }

    /**
     * Get child pid pool.
     *
     * @return \ARedisSet
     */
    protected function getChildPool()
    {
        if ($this->childPool === null) {
            $this->childPool = \Yii::app()->yiiq->pools->children[$this->pid];
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
                && (\Yii::app()->yiiq->health->isPidAlive($oldPid))
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

        \Yii::app()->yiiq->pools->pids->add($this->pid);
    }

    /**
     * Remove current pid from redis.
     */
    protected function clearPid()
    {
        foreach ($this->queues as $queue) {
            \Yii::app()->redis->del($this->getWorkerPidName($queue));
        }

        \Yii::app()->yiiq->pools->pids->remove($this->pid);
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

    /**
     * Signal handlers array.
     *
     * @return string[]
     */
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
     * @param integer $options (optional)
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

            $job = $this->pidToJob[$childPid];
            unset($this->pidToJob[$childPid]);

            // If status is non-zero or job is still marked as executing,
            // child process failed.

            if ($status || $job->status->isExecuting) {
                \Yii::app()->yiiq->restore($job->id);
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
     * @param Job $job
     */
    protected function runJob(Job $job)
    {
        $runner = new Runner(\Yii::app()->yiiq, $job);

        $childPid = $runner->run();
        if (!$childPid) {
            return;
        }

        $this->pidToJob[$childPid] = $job;
        $this->getChildPool()->add($childPid);
    }

    /**
     * Main loop.
     */
    protected function loop()
    {
        $count  = count($this->queues);
        $queue  = null;
        $job    = null;

        $queueIterator = new \InfiniteIterator(new \ArrayIterator($this->queues));

        while (!$this->shutdown) {
            // Iterate over free threads.
            while ($this->hasFreeThread() && !$this->shutdown) {
                // Handle signals.
                $this->dispatchSignals();

                if ($this->shutdown) {
                    break;
                }

                // Look for new job.
                // Iterate over all watched queues, stop when new job found or
                // all queues iterated.
                $iterationCount = 0;
                foreach ($queueIterator as $queue) {
                    if ($job = \Yii::app()->yiiq->popJob($queue)) {
                        break;
                    }

                    $iterationCount++;
                    if ($iterationCount >= $count) {
                        break;
                    }
                }

                // No job was found - exit loop.
                if (!$job) {
                    break;
                }

                // Execute found job.
                $this->runJob($job);
            }

            if ($this->shutdown) {
                break;
            }

            // If no free slots available wait for any child to exit. Otherwise just wait some time.
            if (!$this->hasFreeThread()) {
                \Yii::app()->yiiq->setProcessTitle(
                    'worker',
                    $this->queues,
                    'no free threads ('.$this->maxThreads.' threads)'
                );

                $this->waitForThread();
            } else {
                \Yii::app()->yiiq->setProcessTitle(
                    'worker',
                    $this->queues,
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
     * @param string[] $queue
     * @param integer  $threads
     */
    public function actionRun(array $queue, $threads)
    {
        asort($queue);

        $this->pid          = posix_getpid();
        $this->queues       = $queue;
        $this->maxThreads   = (int) $threads;

        \Yii::app()->yiiq->setProcessTitle('worker', $this->queues, 'initializing');

        $this->checkRunningWorkers();
        $this->setupSignals();
        $this->savePid();

        \Yii::trace('Started new yiiq worker '.$this->pid.' for '.implode(', ', $this->queues).'.');

        $this->loop();

        \Yii::app()->yiiq->setProcessTitle('worker', $this->queues, 'terminating');

        $this->waitForThreads();
        $this->clearPid();

        \Yii::trace('Terminated yiiq worker '.$this->pid.'.');
    }
}
