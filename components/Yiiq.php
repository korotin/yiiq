<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains Yiiq component class.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package ext.yiiq
 */

/**
 * Yiiq component class.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class Yiiq extends CApplicationComponent
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
     * Type name for scheduled tasks.
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
     * Maximum amount of faults per job.
     * Non-repeatable job will be removed if this limit is exceeded.
     * False value means no limit (which is not recommended).
     * 
     * @var integer
     */
    public $maxFaults       = 5;

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
     * Pid pool.
     * 
     * @var ARedisSet
     */
    protected $pidPool      = null;

    /**
     * Array of simple job pools.
     * 
     * @var ARedisSet[]
     */
    protected $queues       = array();

    /**
     * Array of scheduled job pools.
     * 
     * @var ARedisSortedSet[]
     */
    protected $schedules    = array();

    /**
     * Array of repeatable job pools.
     * 
     * @var ARedisSortedSet[]
     */
    protected $repeatables  = array();

    /**
     * Array of executing job pools.
     * 
     * @var ARedisSortedSet[]
     */
    protected $executing = array();

    /**
     * Array of failed job pools.
     * 
     * @var ARedisSet[]
     */
    protected $failed = array();

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

    protected function generateJobId()
    {
        return Yii::app()->redis->incr($this->prefix.':counter');
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
        $lines = array();
        exec('sh -c "ps ax | grep '.$pid.' | grep -v grep"', $lines);
        foreach ($lines as $line) {
            if (preg_match('/(^|\s+)'.$pid.'\s+/', $line)) return true;
        }

        return false;
    }

    /**
     * Get redis set with worker pids.
     * 
     * @return ARedisSet
     */
    public function getPidPool()
    {
        if ($this->pidPool === null) {
            $this->pidPool = new ARedisSet($this->prefix.':pids');
        }

        return $this->pidPool;
    }

    /**
     * Get simple job pool for given queue.
     * 
     * @param  string[optional] $queue Yiiq::DEFAULT_QUEUE by default
     * @return ARedisSet
     */
    protected function getSimplePool($queue = self::DEFAULT_QUEUE)
    {
        if (!$queue) {
            $queue = self::DEFAULT_QUEUE;
        }

        if (!isset($this->queues[$queue])) {
            $this->queues[$queue] = new ARedisSet($this->prefix.':queue:'.$queue.':'.self::TYPE_SIMPLE);
        }

        return $this->queues[$queue];
    }

    /**
     * Get scheduled job pool for given queue.
     * 
     * @param  string $queue
     * @return ARedisSortedSet
     */
    protected function getScheduledPool($queue = self::DEFAULT_QUEUE)
    {
        if (!$queue) {
            $queue = self::DEFAULT_QUEUE;
        }

        if (!isset($this->schedules[$queue])) {
            $this->schedules[$queue] = new ARedisSortedSet($this->prefix.':queue:'.$queue.':'.self::TYPE_SCHEDULED);
        }

        return $this->schedules[$queue];
    }

    /**
     * Get repeatable job pool for given queue.
     * 
     * @param  string $queue
     * @return ARedisSortedSet
     */
    protected function getRepeatablePool($queue = self::DEFAULT_QUEUE)
    {
        if (!$queue) {
            $queue = self::DEFAULT_QUEUE;
        }

        if (!isset($this->repeatables[$queue])) {
            $this->repeatables[$queue] = new ARedisSortedSet($this->prefix.':queue:'.$queue.':'.self::TYPE_REPEATABLE);
        }

        return $this->repeatables[$queue];
    }

    /**
     * Get executing job pool for given queue.
     * 
     * @param  string $queue
     * @return ARedisSortedSet
     */
    protected function getExcutingPool($queue = self::DEFAULT_QUEUE)
    {
        if (!isset($this->executing[$queue])) {
            $this->executing[$queue] = new ARedisSortedSet($this->prefix.':executing:'.$queue);
        }

        return $this->executing[$queue];
    }

    /**
     * Get failed job pool for given queue.
     * 
     * @param  string $queue
     * @return ARedisSet
     */
    protected function getFailedPool($queue = self::DEFAULT_QUEUE)
    {
        if (!isset($this->failed[$queue])) {
            $this->failed[$queue] = new ARedisSet($this->prefix.':failed:'.$queue);
        }

        return $this->failed[$queue];
    }

    /**
     * Get interval for repeatable job.
     * 
     * @param  string $id
     * @return int
     */
    protected function getJobDataInterval($id)
    {
        return (int) Yii::app()->redis->get($this->prefix.':interval:'.$id);
    }

    /**
     * Set interval for repeatable job.
     * 
     * @param string $id
     * @param int $interval
     */
    protected function setJobInterval($id, $interval)
    {
        Yii::app()->redis->set($this->prefix.':interval:'.$id, $interval);
    }

    /**
     * Delete job interval for repeatable job.
     * 
     * @param string $id
     */
    protected function deleteJobInterval($id)
    {
        Yii::app()->redis->del($this->prefix.':interval:'.$id);
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
        return Yii::app()->redis->getClient()->exists($id);
    }

    /**
     * Get job data by id.
     * 
     * @param  string $id
     * @return mixed  YiiqJobData or null
     */
    public function getJobData($id)
    {
        $key = $this->getJobKey($id);
        $job = Yii::app()->redis->getClient()->get($key);
        if (!$job) return;

        return new YiiqJobData($job); 
    }

    /**
     * Save job data to redis.
     * 
     * @param  YiiqJobData  $jobData
     * @param  boolean $overwrite[optional] overwrite existing job, default is false
     * @return mixed            id if job is saved or null
     */
    protected function saveJobData(YiiqJobData $jobData, $overwrite = false)
    {
        if (
            $jobData->id 
            && $this->hasJob($jobData->id) 
            && !$overwrite
        ) return;

        $jobData->id = $jobData->id ?: $this->generateJobId();
        $jobData->created = time();
        
        Yii::app()->redis->getClient()->set(
            $this->getJobKey($jobData->id), 
            (string) $jobData
        );

        return $jobData->id;
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
     * 
     * @param  string $id
     */
    public function deleteJob($id)
    {
        $redis = Yii::app()->redis;
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

        $redis->del($key);
    }

    /**
     * Check whether job with given id is correct.
     * If job is incorrect, it will be deleted.
     * 
     * @param  string $id
     * @return bool   true if job is correct
     */
    protected function checkJob($id)
    {
        if (
            !($jobData = $this->getJobData($id))
            || !$jobData->queue 
            || !$jobData->type
        ) {
            $this->deleteJob($id);
            return false;
        }

        switch ($jobData->type) {
            case self::TYPE_SIMPLE:
                $pool = $this->getSimplePool($jobData->queue);
                break;

            case self::TYPE_SCHEDULED:
                $pool = $this->getScheduledPool($jobData->queue);
                break;

            case self::TYPE_REPEATABLE:
                $pool = $this->getRepeatablePool($jobData->queue);
                break;
        }

        $executingPool = $this->getExcutingPool($jobData->queue);

        if (!$pool->contains($id) && !$executingPool->contains($id)) {
            $this->deleteJob($id);
            return false;
        }

        return true;
    }

    /**
     * Reenqueue job by id.
     * 
     * @param  string $id
     * @return bool   true if job was restored
     */
    protected function restoreJob($id)
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

        if (
            $this->maxFaults 
            && $jobData->faults >= $this->maxFaults
            && $jobData->type !== self::TYPE_REPEATABLE
        ) {
            $this->deleteJob($id);
            $this->getFailedPool($jobData->queue)->add((string) $jobData);

            return false;
        }

        $this->saveJobData($jobData, true);

        switch ($jobData->type) {
            case self::TYPE_SIMPLE:
                $this->getSimplePool($jobData->queue)->add($jobData->id);
                break;

            case self::TYPE_SCHEDULED:
                $pool = $this->getScheduledPool($jobData->queue)->add($jobData->id, $jobData->timestamp);
                break;

            case self::TYPE_REPEATABLE:
                $pool = $this->getRepeatablePool($jobData->queue)->add($jobData->id, time());
                break;
        }

        return true;
    }

    /**
     * Event triggered in forked process before executing job.
     */
    public function onBeforeJob()
    {
        $this->raiseEvent('onBeforeJob', new CEvent($this));
    }

    /**
     * Event triggered in forked process after executing job.
     */
    public function onAfterJob()
    {
        $this->raiseEvent('onAfterJob', new CEvent($this));
    }

    /**
     * Create a simple job in given queue.
     *
     * @param  string $class            job class extended from YiiqBaseJob
     * @param  array[optional]  $args   values for job object properties
     * @param  string[optional] $queue  Yiiq::DEFAULT_QUEUE by default
     * @param  string[optional] $id     globally unique job id             
     * @return mixed                    job id or null if job with same id extists
     */
    public function enqueueJob($class, array $args = array(), $queue = self::DEFAULT_QUEUE, $id = null)
    {
        $jobData = new YiiqJobData;
        $jobData->id = $id;
        $jobData->queue = $queue;
        $jobData->type = self::TYPE_SIMPLE;
        $jobData->class = $class;
        $jobData->args = $args;

        if ($id = $this->saveJobData($jobData)) {
            $this->getSimplePool($queue)->add($id);
        }

        return $id;
    }

    /**
     * Create a job in given queue wich will be executed at given time.
     * 
     * @param  int $timestamp           timestamp to execute job at
     * @param  string $class            job class extended from YiiqBaseJob
     * @param  array[optional]  $args   values for job object properties
     * @param  string[optional] $queue  Yiiq::DEFAULT_QUEUE by default
     * @param  string[optional] $id     globally unique job id             
     * @return mixed                    job id or null if job with same id extists
     */
    public function enqueueJobAt($timestamp, $class, array $args = array(), $queue = self::DEFAULT_QUEUE, $id = null)
    {
        $jobData = new YiiqJobData;
        $jobData->id = $id;
        $jobData->queue = $queue;
        $jobData->type = self::TYPE_SCHEDULED;
        $jobData->class = $class;
        $jobData->args = $args;
        $jobData->timestamp = $timestamp;

        if ($id = $this->saveJobData($jobData)) {
            $this->getScheduledPool($queue)->add($id, $timestamp);
        }

        return $id;
    }

    /**
     * Create a job in given queue wich will be executed after given amount of seconds.
     * 
     * @param  int $interval            amount of seconds to wait before execution
     * @param  string $class            job class extended from YiiqBaseJob
     * @param  array[optional]  $args   values for job object properties
     * @param  string[optional] $queue  Yiiq::DEFAULT_QUEUE by default
     * @param  string[optional] $id     globally unique job id             
     * @return mixed                    job id or null if job with same id extists
     */
    public function enqueueJobIn($interval, $class, array $args = array(), $queue = self::DEFAULT_QUEUE, $id = null)
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
    public function enqueueRepeatableJob($id, $interval, $class, array $args = array(), $queue = self::DEFAULT_QUEUE)
    {
        $interval = (int) floor($interval);

        $jobData = new YiiqJobData;
        $jobData->id = $id;
        $jobData->queue = $queue;
        $jobData->type = self::TYPE_REPEATABLE;
        $jobData->class = $class;
        $jobData->args = $args;
        $jobData->interval = $interval;

        $this->saveJobData($jobData, true);
        $this->setJobInterval($id, $interval);
        $this->getRepeatablePool($queue)->add($id, time());

        return $id;
    }

    /**
     * Pop simple job id from given queue.
     * 
     * @param  string       $queue
     * @return mixed        YiiqJobData or null
     */
    protected function popSimpleJob($queue)
    {
        if (!($id = $this->getSimplePool($queue)->pop())) return;

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
        $redis = Yii::app()->redis;

        $ids = $redis->zrangebyscore($schedule->name, 0, time(), 'LIMIT', 0, 1);
        if (!$ids) return;
        
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
        $redis = Yii::app()->redis;

        $ids = $redis->zrangebyscore($repeatable->name, 0, time(), 'LIMIT', 0, 1);
        if (!$ids) return;
        
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
        $methods = array(
            'popScheduledJob', 
            'popRepeatableJob', 
            'popSimpleJob',
        );

        foreach ($methods as $method) {
            $jobData = $this->$method($queue);
            if ($jobData) return $jobData;
        }
    }

    /**
     * Mark job as started.
     * Job id will be added to executing pool for current queue.
     * 
     * @param  YiiqJobData  $jobData
     * @param  int          $pid        forked worker pid
     */
    public function markAsStarted(YiiqJobData $jobData, $pid)
    {
        $this->getExcutingPool($jobData->queue)->add($jobData->id, $pid);
    }

    /**
     * Mark job as completed.
     * Jod id will be removed from exetuting pool and job data will be deleted
     * for non-repeatable jobs.
     * 
     * @param  YiiqJobData  $jobData
     */
    public function markAsCompleted(YiiqJobData $jobData)
    {
        $this->getExcutingPool($jobData->queue)->remove($jobData->id);
        if ($jobData->type !== self::TYPE_REPEATABLE) {
            $this->deleteJob($jobData->id);
        }
    }

    /**
     * Remove dead process pids from redis set.
     * 
     * @param  ARedisSet    $pool
     * @return int          amount of dead pids
     */
    public function checkPidPool(ARedisSet $pool)
    {
        $removed = 0;
        $pids = $pool->getData(true);
        foreach ($pids as $pid) {
            if ($this->isPidAlive($pid)) continue;
            $pool->remove($pid);
            $removed++;
        }

        return $removed;
    }

    /**
     * Check for stopped jobs ib given queue.
     * 
     * @param  string $queue
     * @return array            [found jobs, restored jobs]
     */
    public function checkStoppedJobs($queue)
    {
        $pool = $this->getExcutingPool($queue);

        $found = 0;
        $restored = 0;
        $jobs = $pool->getData(true);
        foreach ($jobs as $id => $pid) {
            if ($this->isPidAlive($pid)) continue;
            $pool->remove($id);
            $found++;
            if ($this->restoreJob($id)) $restored++;
        }

        return array($found, $restored);
    }

    /**
     * Check redis db consistency.
     * 
     * Remove dead worker pids from pid pool, remove lost jobs 
     * and restore stopped jobs.
     */
    public function check($log = true)
    {
        if ($log) echo "Checking for dead children... ";
        $deadChildren = 0;
        $keys = Yii::app()->redis->keys($this->prefix.':children:*');
        foreach ($keys as $key) {
            if (Yii::app()->redis->prefix) {
                $key = mb_substr($key, mb_strlen(Yii::app()->redis->prefix));
            }
            $deadChildren += $this->checkPidPool(new ARedisSet($key));
        }
        if ($log) echo "$deadChildren found.\n";

        if ($log) echo "Checking for dead workers... ";
        $deadWorkers = $this->checkPidPool($this->getPidPool());
        if ($log) echo "$deadWorkers found.\n";

        if ($log) echo "Checking for lost jobs... ";
        $lostJobs = 0;
        $keys = Yii::app()->redis->keys($this->prefix.':job:*');
        foreach ($keys as $key) {
            $id = null;
            if (!preg_match('/:([a-z0-9]+)$/', $key, $id)) continue;
            $id = $id[1];

            if ($this->checkJob($id)) continue;
            $lostJobs++;
        }
        if ($log) echo "$lostJobs found.\n";

        if ($log) echo "Checking for stopped jobs... ";
        $keys = Yii::app()->redis->keys($this->prefix.':executing:*');

        $foundJobs = 0;
        $restoredJobs = 0;
        foreach ($keys as $key) {
            $queue = null;
            if (!preg_match('/\:([^:]+)$/', $key)) continue;
            $queue = $queue[1];

            $result = $this->checkStoppedJobs($queue);
            $foundJobs += $result[0];
            $restoredJobs += $result[1];
        }
        if ($log) echo "$foundJobs found, $restoredJobs restored.\n";
    }

}