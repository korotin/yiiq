<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains Yiiq component class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq
 */

namespace Yiiq;

use Yiiq\jobs\Data;
use Yiiq\jobs\Producer;

/**
 * Yiiq component class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class Yiiq extends \CApplicationComponent
{
    /**
     * Default queue name.
     */
    const DEFAULT_QUEUE     = 'default';

    /**
     * Default thread count per worker.
     */
    const DEFAULT_THREADS   = 5;

    /**
     * Commands for broadcasting.
     */
    const COMMAND_NEWJOB    = 'newjob';
    const COMMAND_EXIT      = 'exit';

    /**
     * Type names for scheduled tasks.
     */
    const TYPE_SIMPLE       = 'simple';
    const TYPE_SCHEDULED    = 'scheduled';
    const TYPE_REPEATABLE   = 'repeatable';

    /**
     * Yiiq instance name.
     * Used in process title.
     *
     * @var string
     */
    public $name            = null;

    /**
     * Yiiq redis keys prefix.
     * Should not be ended with ":".
     *
     * @var string
     */
    public $prefix          = 'yiiq';

    /**
     * Array of intervals (in seconds) between job faults.
     * Job will be removed if all intervals are passed.
     *
     * @var integer[]
     */
    public $faultIntervals  = [30, 60, 300, 3600, 7200];

    /**
     * Process title template.
     * Available placeholders are:
     *     {name}       - instance name or application path
     *     {type}       - process type (worker or job)
     *     {queue}      - queue name
     *     {message}    - title message
     *
     * @var string
     */
    public $titleTemplate   = 'Yiiq [{name}] {type}@{queue}: {message}';

    /**
     * Broadcast channel.
     * Not used for now!
     *
     * @var \ARedisChannel
     */
    protected $broadcast    = null;

    /**
     * Pid pool.
     *
     * @var \ARedisSet
     */
    protected $pidPool      = null;

    /**
     * Array of simple job pools.
     *
     * @var \ARedisSet[]
     */
    protected $simplePool   = [];

    /**
     * Array of scheduled job pools.
     *
     * @var \ARedisSortedSet[]
     */
    protected $scheduledPool = [];

    /**
     * Array of repeatable job pools.
     *
     * @var \ARedisSortedSet[]
     */
    protected $repeatablePool= [];

    /**
     * Executing job pool.
     *
     * @var \ARedisSortedSet
     */
    protected $executing = null;

    /**
     * Completed job pool.
     *
     * @var \ARedisSet
     */
    protected $completed = null;

    /**
     * Array of failed job pools.
     *
     * @var \ARedisSet
     */
    protected $failed = null;

    /**
     * Get redis job key for given id.
     *
     * @param  string $id
     * @return string
     */
    protected function getJobKey($id)
    {
        return $this->prefix.':job:'.$id;
    }

    /**
     * Get redis job result key for given id.
     *
     * @param  string $id
     * @return string
     */
    protected function getJobResultKey($id)
    {
        return $this->prefix.':result:'.$id;
    }

    /**
     * Generate job id by increment.
     *
     * @return string
     */
    protected function generateJobId()
    {
        return \Yii::app()->redis->incr($this->prefix.':counter');
    }

    /**
     * Get broadcasting channel.
     *
     * @return \ARedisChannel
     */
    protected function getBroadcast()
    {
        if ($this->broadcast === null) {
            $this->broadcast = new \ARedisChannel($this->prefix.':broadcast');
        }

        return $this->broadcast;
    }

    /**
     * Send message to broadcasting channel.
     *
     * @param  string $message
     * @param  array[optional] $params
     */
    public function sendMessage($message, array $params = [])
    {
        $this->getBroadcast()->publish(\CJSON::encode(compact('message', 'params')));
    }

    /**
     * Subscribe to broadcasting channel.
     * This is a blocking operation.
     *
     * @param  callable $callback callback has a signature func(Yiiq $yiiq, $message, array $params)
     */
    public function subscribe($callback)
    {
        $broadcast = $this->getBroadcast();
        $broadcast->onReceiveMessage = function ($event) use ($callback) {
            $message = $event->sender->getLastMessage();
            $message = \CJSON::decode($message);
            $callback($this, $message['message'], $message['params']);
        };
        $broadcast->subscribe();
    }

    /**
     * Unsubscribe from broadcasting channel.
     */
    public function unsubscribe()
    {
        $this->getBroadcast()->unsubscribe();
    }

    /**
     * Is process with given pid alive?
     *
     * @param  int  $pid
     * @return boolean
     */
    public function isPidAlive($pid)
    {
        $pid = (int) $pid;
        $lines = [];
        exec('sh -c "ps ax | grep '.$pid.' | grep -v grep"', $lines);
        foreach ($lines as $line) {
            if (preg_match('/(^|\s+)'.$pid.'\s+/', $line)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Get redis set with worker pids.
     *
     * @return \ARedisSet
     */
    public function getPidPool()
    {
        if ($this->pidPool === null) {
            $this->pidPool = new \ARedisSet($this->prefix.':pids');
        }

        return $this->pidPool;
    }

    /**
     * Get simple job pool for given queue.
     *
     * @param  string[optional] $queue Yiiq::DEFAULT_QUEUE by default
     * @return \ARedisSet
     */
    protected function getSimplePool($queue = self::DEFAULT_QUEUE)
    {
        if (!$queue) {
            $queue = self::DEFAULT_QUEUE;
        }

        if (!isset($this->simplePool[$queue])) {
            $this->simplePool[$queue] =
                new \ARedisSet($this->prefix.':queue:'.$queue.':'.self::TYPE_SIMPLE);
        }

        return $this->simplePool[$queue];
    }

    /**
     * Get scheduled job pool for given queue.
     *
     * @param  string $queue
     * @return \ARedisSortedSet
     */
    protected function getScheduledPool($queue = self::DEFAULT_QUEUE)
    {
        if (!$queue) {
            $queue = self::DEFAULT_QUEUE;
        }

        if (!isset($this->scheduledPool[$queue])) {
            $this->scheduledPool[$queue] =
                new \ARedisSortedSet($this->prefix.':queue:'.$queue.':'.self::TYPE_SCHEDULED);
        }

        return $this->scheduledPool[$queue];
    }

    /**
     * Get repeatable job pool for given queue.
     *
     * @param  string $queue
     * @return \ARedisSortedSet
     */
    protected function getRepeatablePool($queue = self::DEFAULT_QUEUE)
    {
        if (!$queue) {
            $queue = self::DEFAULT_QUEUE;
        }

        if (!isset($this->repeatablePool[$queue])) {
            $this->repeatablePool[$queue] =
                new \ARedisSortedSet($this->prefix.':queue:'.$queue.':'.self::TYPE_REPEATABLE);
        }

        return $this->repeatablePool[$queue];
    }

    /**
     * Get executing job pool for given queue.
     *
     * @return \ARedisSortedSet
     */
    public function getExecutingPool()
    {
        if ($this->executing === null) {
            $this->executing = new \ARedisSortedSet($this->prefix.':executing');
        }

        return $this->executing;
    }

    /**
     * Get completed job pool.
     *
     * @return \ARedisSet
     */
    protected function getCompletedPool()
    {
        if ($this->completed === null) {
            $this->completed = new \ARedisSet($this->prefix.':completed');
        }

        return $this->completed;
    }

    /**
     * Get failed job pool.
     *
     * @return \ARedisSet
     */
    protected function getFailedPool()
    {
        if ($this->failed === null) {
            $this->failed = new \ARedisSet($this->prefix.':failed');
        }

        return $this->failed;
    }

    /**
     * Get interval for repeatable job.
     *
     * @param  string $id
     * @return int
     */
    protected function getJobDataInterval($id)
    {
        return (int) \Yii::app()->redis->get($this->prefix.':interval:'.$id);
    }

    /**
     * Set interval for repeatable job.
     *
     * @param string $id
     * @param int $interval
     */
    protected function setJobInterval($id, $interval)
    {
        \Yii::app()->redis->set($this->prefix.':interval:'.$id, $interval);
    }

    /**
     * Delete job interval for repeatable job.
     *
     * @param string $id
     */
    protected function deleteJobInterval($id)
    {
        \Yii::app()->redis->del($this->prefix.':interval:'.$id);
    }

    /**
     * Get job data by id.
     *
     * @param  string $id
     * @return mixed  \Yiiq\jobs\Data or null
     */
    protected function getJobData($id)
    {
        $key = $this->getJobKey($id);
        $job = \Yii::app()->redis->getClient()->get($key);
        if (!$job) {
            return;
        }

        return new Data($job);
    }

    /**
     * Save job data to redis.
     *
     * @param  Yiiq\jobs\Data  $jobData
     * @param  boolean $overwrite[optional] overwrite existing job, default is false
     * @return mixed            id if job is saved or null
     */
    protected function saveJobData(Data $jobData, $overwrite = false)
    {
        if (
            $jobData->id
            && $this->hasJob($jobData->id)
            && !$overwrite
        ) {
            return;
        }

        $jobData->id = $jobData->id ?: $this->generateJobId();
        $jobData->created = time();

        $saved = \Yii::app()->redis->getClient()->set(
            $this->getJobKey($jobData->id),
            (string) $jobData
        );

        if (!$saved) {
            return;
        }

        return $jobData->id;
    }

    /**
     * Save job result to redis.
     * Empty results will not be saved.
     *
     * @param  string $id
     * @param  mixed $result
     */
    protected function saveJobResult($id, $result)
    {
        if ($result === null) {
            return;
        }

        $key = $this->getJobResultKey($id);
        $result = \CJSON::encode($result);

        \Yii::app()->redis->getClient()->set(
            $key,
            $result
        );
    }

    /**
     * Get job result.
     *
     * @param  string  $id
     * @param  boolean $clear delete result from redis
     * @return mixed
     */
    public function getJobResult($id, $clear = false)
    {
        $key = $this->getJobResultKey($id);
        $raw = \Yii::app()->redis->getClient()->get($key);

        if ($raw && $clear) {
            $this->clearJobResult($id);
        }

        return $raw ? \CJSON::decode($raw) : null;
    }

    /**
     * Delete job result from redis.
     *
     * @param  string $id
     */
    public function clearJobResult($id)
    {
        $key = $this->getJobResultKey($id);
        \Yii::app()->redis->getClient()->del($key);
    }

    /**
     * Does job with given id exist.
     *
     * @param  string  $id
     * @return boolean
     */
    public function hasJob($id)
    {
        $key = $this->getJobKey($id);
        return \Yii::app()->redis->getClient()->exists($key);
    }

    /**
     * Is job with given id executing at moment.
     *
     * @param  string  $id
     * @return boolean
     */
    public function isExecuting($id)
    {
        return \Yii::app()->redis->getClient()->zrank(
            $this->getExecutingPool()->name,
            $id
        ) !== false;
    }

    /**
     * Is job with given id completed successfully.
     *
     * @param  string  $id
     * @return boolean
     */
    public function isCompleted($id)
    {
        return (bool) \Yii::app()->redis->getClient()->sismember(
            $this->getCompletedPool()->name,
            $id
        );
    }

    /**
     * Is job with given id failed.
     *
     * @param  string  $id
     * @return boolean
     */
    public function isFailed($id)
    {
        return (bool) \Yii::app()->redis->getClient()->sismember(
            $this->getFailedPool()->name,
            $id
        );
    }

    /**
     * Delete simple job.
     *
     * @param  string $queue
     * @param  string $id
     */
    protected function deleteSimpleJob($queue, $id)
    {
        $this->getSimplePool($queue)->remove($id);
    }

    /**
     * Delete scheduled job.
     *
     * @param  string $queue
     * @param  string $id
     */
    protected function deleteScheduledJob($queue, $id)
    {
        $this->getScheduledPool($queue)->remove($id);
    }

    /**
     * Delete repeatable job.
     *
     * @param  string $queue
     * @param  string $id
     */
    protected function deleteRepeatableJob($queue, $id)
    {
        $this->getRepeatablePool($queue)->remove($id);
        $this->deleteJobInterval($id);
    }

    /**
     * Delete job by id.
     * By default ($withData = true) job data will be also
     * deleted.
     *
     * @param  string $id
     * @param  bool[optional] $withData
     */
    public function deleteJob($id, $withData = true)
    {
        $redis = \Yii::app()->redis;
        $job = $this->getJobData($id);
        $key = $this->getJobKey($id);

        if (isset($job->queue) && isset($job->type)) {
            switch ($job->type) {
                case self::TYPE_SIMPLE:
                    $this->deleteSimpleJob($job->queue, $id);
                    break;

                case self::TYPE_SCHEDULED:
                    $this->deleteScheduledJob($job->queue, $id);
                    break;

                case self::TYPE_REPEATABLE:
                    $this->deleteRepeatableJob($job->queue, $id);
                    break;
            }
        }

        if ($withData) {
            $redis->del($key);
        }
    }

    /**
     * Reenqueue job by id.
     *
     * @param  string $id
     * @return bool   true if job was restored
     */
    public function restoreJob($id)
    {
        if (
            !($jobData = $this->getJobData($id))
            || !$jobData->queue
            || !$jobData->type
        ) {
            return false;
        }

        $jobData->faults++;
        $jobData->lastFailed = time();

        $this->getExecutingPool()->remove($id);

        if (
            $this->faultIntervals
            && $jobData->faults > count($this->faultIntervals)
        ) {
            $this->saveJobData($jobData, true);
            $this->deleteJob($id, false);
            $this->getFailedPool()->add($id);
            return false;
        }

        $timestamp = $jobData->lastFailed + $this->faultIntervals[$jobData->faults - 1];

        switch ($jobData->type) {
            case self::TYPE_SIMPLE:
                $jobData->type = self::TYPE_SCHEDULED;
                $jobData->timestamp = $timestamp;
                // no break

            case self::TYPE_SCHEDULED:
                $this->getScheduledPool($jobData->queue)->add($jobData->id, $timestamp);
                break;

            case self::TYPE_REPEATABLE:
                $this->getRepeatablePool($jobData->queue)->add($jobData->id, $timestamp);
                break;
        }

        $this->saveJobData($jobData, true);

        return true;
    }

    /**
     * Event triggered in forked process before executing job.
     */
    public function onBeforeJob()
    {
        $this->raiseEvent('onBeforeJob', new \CEvent($this));
    }

    /**
     * Event triggered in forked process after executing job.
     */
    public function onAfterJob()
    {
        $this->raiseEvent('onAfterJob', new \CEvent($this));
    }

    /**
     * Create a simple job in given queue.
     *
     * @param  string $class            job class extended from \Yiiq\jobs\Base
     * @param  array[optional]  $args   values for job object properties
     * @param  string[optional] $queue  \Yiiq\Yiiq::DEFAULT_QUEUE by default
     * @param  string[optional] $id     globally unique job id
     * @return mixed                    job id or null if job with same id extists
     */
    public function enqueueJob($class, array $args = [], $queue = self::DEFAULT_QUEUE, $id = null)
    {
        $jobData = new Data;
        $jobData->id = $id;
        $jobData->queue = $queue;
        $jobData->type = self::TYPE_SIMPLE;
        $jobData->class = $class;
        $jobData->args = $args;

        if ($id = $this->saveJobData($jobData)) {
            $this->getSimplePool($queue)->add($id);
            $this->sendMessage(self::COMMAND_NEWJOB, [$queue]);
        }

        return $id;
    }

    /**
     * Create a job in given queue wich will be executed at given time.
     *
     * @param  int $timestamp           timestamp to execute job at
     * @param  string $class            job class extended from \Yiiq\jobs\Base
     * @param  array[optional]  $args   values for job object properties
     * @param  string[optional] $queue  \Yiiq\Yiiq::DEFAULT_QUEUE by default
     * @param  string[optional] $id     globally unique job id
     * @return mixed                    job id or null if job with same id extists
     */
    public function enqueueJobAt($timestamp, $class, array $args = [], $queue = self::DEFAULT_QUEUE, $id = null)
    {
        $jobData = new Data;
        $jobData->id = $id;
        $jobData->queue = $queue;
        $jobData->type = self::TYPE_SCHEDULED;
        $jobData->class = $class;
        $jobData->args = $args;
        $jobData->timestamp = $timestamp;

        if ($id = $this->saveJobData($jobData)) {
            $this->getScheduledPool($queue)->add($id, $timestamp);
            $this->sendMessage(self::COMMAND_NEWJOB, [$queue]);
        }

        return $id;
    }

    /**
     * Create a job in given queue wich will be executed after given amount of seconds.
     *
     * @param  int $interval            amount of seconds to wait before execution
     * @param  string $class            job class extended from \Yiiq\jobs\Base
     * @param  array[optional]  $args   values for job object properties
     * @param  string[optional] $queue  \Yiiq\Yiiq::DEFAULT_QUEUE by default
     * @param  string[optional] $id     globally unique job id
     * @return mixed                    job id or null if job with same id extists
     */
    public function enqueueJobIn($interval, $class, array $args = [], $queue = self::DEFAULT_QUEUE, $id = null)
    {
        $interval = (int) floor($interval);
        return $this->enqueueJobAt(time() + $interval, $class, $args, $queue, $id);
    }

    /**
     * Create a job, which will execute each $interval seconds.
     *
     * Unlike other job types repeatable job requires $id to be set.
     * If job with given id already exists, it will be overwritten.
     *
     * @param  string       $id
     * @param  integer      $interval
     * @param  string       $class
     * @param  array        $args[optional]
     * @param  string       $queue[optional]
     * @return string
     */
    public function enqueueRepeatableJob($id, $interval, $class, array $args = [], $queue = self::DEFAULT_QUEUE)
    {
        $interval = (int) floor($interval);

        $jobData = new Data;
        $jobData->id = $id;
        $jobData->queue = $queue;
        $jobData->type = self::TYPE_REPEATABLE;
        $jobData->class = $class;
        $jobData->args = $args;
        $jobData->interval = $interval;

        $this->saveJobData($jobData, true);
        $this->setJobInterval($id, $interval);
        $this->getRepeatablePool($queue)->add($id, time());
        $this->sendMessage(self::COMMAND_NEWJOB, [$queue]);

        return $id;
    }

    /**
     * Create a job via job producer.
     *
     * @param  string $class
     * @return \Yiiq\jobs\Producer
     */
    public function createJob($class)
    {
        return new Producer($this, $class);
    }

    /**
     * Enqueue job by job producer.
     *
     * Called by \Yiiq\jobs\Producer.
     * Returns job id.
     *
     * @param  \Yiiq\jobs\Producer $producer
     * @return string
     */
    public function enqueueJobByProducer(Producer $producer)
    {
        switch ($jobProducer->type) {
            case self::TYPE_SIMPLE:
                return $this->enqueueJob(
                    $producer->class,
                    $producer->args,
                    $producer->queue,
                    $producer->id
                );
                break;

            case self::TYPE_SCHEDULED:
                if ($producer->timestamp) {
                    return $this->enqueueJobAt(
                        $producer->timestamp,
                        $producer->class,
                        $producer->args,
                        $producer->queue,
                        $producer->id
                    );
                } else {
                    return $this->enqueueJobIn(
                        $producer->interval,
                        $producer->class,
                        $producer->args,
                        $producer->queue,
                        $producer->id
                    );
                }
                break;

            case self::TYPE_REPEATABLE:
                return $this->enqueueRepeatableJob(
                    $producer->id,
                    $producer->interval,
                    $producer->class,
                    $producer->args,
                    $producer->queue
                );
                break;
        }
    }

    /**
     * Pop simple job id from given queue.
     *
     * @param  string       $queue
     * @return mixed        YiiqJobData or null
     */
    protected function popSimpleJob($queue)
    {
        if (!($id = $this->getSimplePool($queue)->pop())) {
            return;
        }

        return $this->getJobData($id);
    }

    /**
     * Pop scheduled job id from given queue.
     * FIXME this action is not atomic
     *
     * @param  string       $queue
     * @return mixed        YiiqJobData or null
     */
    protected function popScheduledJob($queue)
    {
        $schedule = $this->getScheduledPool($queue);
        $redis = \Yii::app()->redis;

        $ids = $redis->zrangebyscore($schedule->name, 0, time(), ['limit' => [0, 1]]);
        if (!$ids) {
            return;
        }

        $id = reset($ids);
        $redis->zrem($schedule->name, $id);

        return $this->getJobData($id);
    }

    /**
     * Pop repeatable job id from given queue.
     *
     * @param  string       $queue
     * @return mixed        YiiqJobData or null
     */
    protected function popRepeatableJob($queue)
    {
        $repeatable = $this->getRepeatablePool($queue);
        $redis = \Yii::app()->redis;

        $ids = $redis->zrangebyscore($repeatable->name, 0, time(), ['limit' => [0, 1]]);
        if (!$ids) {
            return;
        }

        $id = reset($ids);
        $repeatable->add($id, time() + $this->getJobDataInterval($id));

        return $this->getJobData($id);
    }

    /**
     * Pop schedlued or simple job id from given queue.
     *
     * @param  string[optional] $queue Yiiq::DEFAULT_QUEUE by default
     * @return array
     */
    public function popJob($queue = self::DEFAULT_QUEUE)
    {
        $methods = [
            'popScheduledJob',
            'popRepeatableJob',
            'popSimpleJob',
        ];

        foreach ($methods as $method) {
            $jobData = $this->$method($queue);
            if ($jobData) {
                return $jobData;
            }
        }
    }

    /**
     * Mark job as started.
     * Job id will be added to executing pool for current queue.
     *
     * @param  \Yiiq\jobs\Data  $jobData
     * @param  int              $pid        forked worker pid
     */
    public function markAsStarted(Data $jobData, $pid)
    {
        $this->getExecutingPool()->add($jobData->id, $pid);
    }

    /**
     * Mark job as completed.
     * Jod id will be removed from executing pool and job data will be deleted
     * for non-repeatable jobs.
     *
     * @param  \Yiiq\jobs\Data  $jobData
     * @param  mixed            $result
     */
    public function markAsCompleted(Data $jobData, $result)
    {
        $this->getExecutingPool()->remove($jobData->id);
        if ($jobData->type === self::TYPE_REPEATABLE) {
            return;
        }

        $this->getCompletedPool()->add($jobData->id);
        $this->deleteJob($jobData->id);

        $this->saveJobResult($jobData->id, $result);
    }

    /**
     * Remove dead process pids from redis set.
     *
     * @param  \ARedisSet   $pool
     * @return int          amount of dead pids
     */
    public function checkPidPool(\ARedisSet $pool)
    {
        $removed = 0;
        $pids = $pool->getData(true);
        foreach ($pids as $pid) {
            if ($this->isPidAlive($pid)) {
                continue;
            }
            $pool->remove($pid);
            $removed++;
        }

        return $removed;
    }

    /**
     * Check for stopped jobs.
     *
     * @return array            [found jobs, restored jobs]
     */
    public function checkStoppedJobs()
    {
        $pool = $this->getExecutingPool();

        $found = 0;
        $restored = 0;
        $jobs = $pool->getData(true);
        foreach ($jobs as $id => $pid) {
            if ($this->isPidAlive($pid)) {
                continue;
            }
            $found++;
            if ($this->restoreJob($id)) {
                $restored++;
            }
        }

        return [$found, $restored];
    }

    /**
     * Check redis db consistency.
     *
     * Remove dead worker pids from pid pool, remove lost jobs
     * and restore stopped jobs.
     *
     * @param bool[optional] $log
     * @return bool true if no errors found
     */
    public function check($log = true)
    {
        if ($log) {
            echo "Checking for dead children... ";
        }
        $deadChildren = 0;
        $keys = \Yii::app()->redis->keys($this->prefix.':children:*');
        foreach ($keys as $key) {
            if (\Yii::app()->redis->prefix) {
                $key = mb_substr($key, mb_strlen(\Yii::app()->redis->prefix));
            }
            $deadChildren += $this->checkPidPool(new \ARedisSet($key));
        }
        if ($log) {
            echo "$deadChildren found.\n";
        }

        if ($log) {
            echo "Checking for dead workers... ";
        }
        $deadWorkers = $this->checkPidPool($this->getPidPool());
        if ($log) {
            echo "$deadWorkers found.\n";
        }

        if ($log) {
            echo "Checking for stopped jobs... ";
        }
        list($stoppedJobs, $restoredJobs) = $this->checkStoppedJobs();
        if ($log) {
            echo "$stoppedJobs found, $restoredJobs restored.\n";
        }

        return
            $deadChildren === 0
            && $deadWorkers === 0
            && $stoppedJobs === 0;
    }
}
