<?php
require_once __DIR__.'/YiiqBaseCommand.php';

class YiiqWorkerCommand extends YiiqBaseCommand
{
    public $maxThreads  = 5;

    protected $shutdown = false;

    protected $pid;
    protected $queue;
    protected $childPool;

    protected function getChildPool()
    {
        if ($this->childPool === null) {
            $this->childPool = new ARedisSet(Yii::app()->yiiq->prefix.':children:'.$this->pid);
        }

        return $this->childPool;
    }

    protected function setupSignals()
    {
        declare(ticks = 1);
        // Set shutdown flag on terminate command
        pcntl_signal(SIGTERM, function() {
            $this->shutdown = true;
        });
        // Grab child process status to prevent zombie processes
        pcntl_signal(SIGCHLD, function() {
            $status = null;
            pcntl_waitpid(-1, $status, WNOHANG);
            pcntl_wexitstatus($status);
        });
    }

    protected function getThreadsCount()
    {
        return (int)Yii::app()->redis->getClient()->scard($this->getChildPool()->name);
    }

    protected function hasFreeThread()
    {
        return $this->getThreadsCount() < $this->maxThreads;
    }

    protected function popJobId()
    {
        return Yii::app()->yiiq->getQueue($this->queue)->pop();
    }

    protected function runJob($jobId)
    {
        $job = Yii::app()->yiiq->getJob($jobId);
        if (!$job) {
            Yii::log('Job '.$jobId.' is missing.', CLogger::LEVEL_WARNING);
        }

        // If we're in parent process, return
        if (pcntl_fork() !== 0) return;

        $childPid = posix_getpid();

        // Force reconnect to redis
        Yii::app()->redis->setClient(null);

        $this->getChildPool()->add($childPid);

        Yii::trace('Forked process for job '.$jobId.'.');
        if (!is_array($job)) Yii::trace($job);

        extract($job);
        Yii::trace('Starting job '.$jobId.' ('.$class.')...');

        $job = new $class;
        $job->execute($args);

        Yii::trace('Job '.$jobId.' done.');
        Yii::app()->yiiq->deleteJob($jobId);

        $this->getChildPool()->remove($childPid);

        exit(0);
    }
    
    public function actionStart($queue = null)
    {
        $this->pid = posix_getpid();
        $this->queue = $queue;
        $this->setupSignals();

        Yii::app()->yiiq->pidPool->add($this->pid);

        Yii::trace('Started new yiiq worker '.$this->pid.' for queue '.($this->queue ?: Yiiq::DEFALUT_QUEUE).'.');

        $status = null;
        while (true) {
            while (
                !$this->shutdown
                && $this->hasFreeThread()
            ) {
                if (!($jobId = $this->popJobId())) break;
                $this->runJob($jobId);
                // Sleep to let child thread update childPool.
                usleep(100000);
            }

            if ($this->shutdown) break;

            if ($this->getThreadsCount()) {
                pcntl_waitpid(-1, $status);
            }
            else {
                sleep(1);
            }
        }

        Yii::trace('Waiting for child threads to terminate...');
        while (pcntl_waitpid(-1, $status) > 0) {
            pcntl_wexitstatus($status);
        }

        Yii::app()->yiiq->pidPool->remove($this->pid);
        Yii::trace('Terminated yiiq worker '.$this->pid.'.');
    }

}