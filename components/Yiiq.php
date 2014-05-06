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
     * Yiiq redis keys prefix.
     * Should not be ended with ":".
     * 
     * @var string
     */
    public $prefix          = 'yiiq';

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
     * Serialize job.
     *
     * @param  string $queue
     * @param  string $type
     * @param  string $class
     * @param  array  $args
     * @param  int $created
     * @return string
     */
    protected function serializeJob($queue, $type, $class, array $args, $created)
    {
        return CJSON::encode(compact('queue', 'type', 'class', 'args', 'created'));
    }

    /**
     * Deserialize job.
     * 
     * @param  string $job
     * @return array
     */
    protected function deserealizeJob($job)
    {
        return CJSON::decode($job);
    }

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
     * Get interval for repeatable job.
     * 
     * @param  string $id
     * @return int
     */
    protected function getJobInterval($id)
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
     * @return mixed        array with class and args fields or null if no job found
     */
    public function getJob($id)
    {
        $key = $this->getJobKey($id);
        $job = Yii::app()->redis->getClient()->get($key);
        if (!$job) return;

        return $this->deserealizeJob($job); 
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
        $job = $this->getJob($id);
        $key = $this->getJobKey($id);

        if (isset($job['queue']) && isset($job['type'])) {
            switch ($job['type']) {
                case self::TYPE_SIMPLE:
                    $this->deleteSimpleJob($job['queue'], $id);
                    break;

                case self::TYPE_SCHEDULED:
                    $this->deleteScheduledJob($job['queue'], $id);
                    break;

                case self::TYPE_REPEATABLE:
                    $this->deleteRepeatableJob($job['queue'], $id);
                    break;
            }
        }

        $redis->del($key);
    }

    /**
     * Save job data to redis.
     * 
     * @param  string  $queue
     * @param  string  $type
     * @param  string  $id
     * @param  string  $class
     * @param  array  $args
     * @param  boolean $overwrite[optional] overwrite existing job, default is false
     * @return mixed            id if job is saved or null
     */
    protected function saveJob($queue, $type, $id, $class, $args, $overwrite = false)
    {
        if ($id && $this->hasJob($id) && !$overwrite) return;
        
        $id     = $id ?: $this->generateJobId();
        $key    = $this->getJobKey($id);
        $job    = $this->serializeJob($queue, $type, $class, $args, time());

        Yii::app()->redis->getClient()->set($key, $job);

        return $id;
    }

    /**
     * Check whether job with given id is correct.
     * If job is incorrect, it will be deleted.
     * 
     * @param  string $id
     * @return bool         true if job is correct
     */
    protected function checkJob($id)
    {
        if (
            !($job = $this->getJob($id))
            || !isset($job['queue']) 
            || !isset($job['type'])
        ) {
            $this->deleteJob($id);
            return false;
        }

        switch ($job['type']) {
            case self::TYPE_SIMPLE:
                $pool = $this->getSimplePool($job['queue']);
                break;

            case self::TYPE_SCHEDULED:
                $pool = $this->getScheduledPool($job['queue']);
                break;

            case self::TYPE_REPEATABLE:
                $pool = $this->getRepeatablePool($job['queue']);
                break;
        }

        if (!$pool->contains($id)) {
            $this->deleteJob($id);
            return false;
        }

        return true;
    }

    /**
     * Create a simple job in given queue.
     *
     * @param  string $class            job class extended from YiiqBaseJob
     * @param  array[optional] $args    values for job object properties
     * @param  string[optional] $queue  Yiiq::DEFAULT_QUEUE by default
     * @param  string[optional] $id     globally unique job id             
     * @return mixed                    job id or null if job with same id extists
     */
    public function enqueueJob($class, array $args = array(), $queue = self::DEFAULT_QUEUE, $id = null)
    {
        if ($id = $this->saveJob($queue, self::TYPE_SIMPLE, $id, $class, $args)) {
            $this->getSimplePool($queue)->add($id);
        }

        return $id;
    }

    /**
     * Create a job in given queue wich will be executed at given time.
     * 
     * @param  int $timestamp           timestamp to execute job at
     * @param  string $class            job class extended from YiiqBaseJob
     * @param  array[optional] $args    values for job object properties
     * @param  string[optional] $queue  Yiiq::DEFAULT_QUEUE by default
     * @param  string[optional] $id     globally unique job id             
     * @return mixed                    job id or null if job with same id extists
     */
    public function enqueueJobAt($timestamp, $class, array $args = array(), $queue = self::DEFAULT_QUEUE, $id = null)
    {
        if ($id = $this->saveJob($queue, self::TYPE_SCHEDULED, $id, $class, $args)) {
            $this->getScheduledPool($queue)->add($id, $timestamp);
        }

        return $id;
    }

    /**
     * Create a job in given queue wich will be executed after given amount of seconds.
     * 
     * @param  int $interval            amount of seconds to wait before execution
     * @param  string $class            job class extended from YiiqBaseJob
     * @param  array[optional] $args    values for job object properties
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
     * @param  string $id
     * @param  integer $interval
     * @param  string $class
     * @param  array  $args[optional]
     * @param  string $queue[optional]
     * @return string
     */
    public function enqueueRepeatableJob($id, $interval, $class, array $args = array(), $queue = self::DEFAULT_QUEUE)
    {
        $this->saveJob($queue, self::TYPE_REPEATABLE, $id, $class, $args, true);

        $interval = (int) floor($interval);
        $this->setJobInterval($id, $interval);
        $this->getRepeatablePool($queue)->add($id, time());

        return $id;
    }

    /**
     * Pop simple job id from given queue.
     * 
     * @param  string $queue
     * @return mixed            job id or null
     */
    protected function popSimpleJob($queue)
    {
        if (!($id = $this->getSimplePool($queue)->pop())) return;

        if ($job = $this->getJob($id)) {
            $job['id'] = $id;
            $this->deleteJob($id);
        }

        return $job;
    }

    /**
     * Pop scheduled job id from given queue.
     * FIXME this action is not atomic
     * 
     * @param  string $queue
     * @return mixed            job id or null
     */
    protected function popScheduledJob($queue)
    {
        $schedule = $this->getScheduledPool($queue);
        $redis = Yii::app()->redis;

        $ids = $redis->zrangebyscore($schedule->name, 0, time(), 'LIMIT', 0, 1);
        if (!$ids) return;
        
        $id = reset($ids);
        $redis->zrem($schedule->name, $id);

        if ($job = $this->getJob($id)) {
            $job['id'] = $id;
            $this->deleteJob($id);
        }

        return $job;
    }

    /**
     * Pop repeatable job id from given queue.
     * 
     * @param  string $queue
     * @return mixed            job id or null
     */
    protected function popRepeatableJob($queue)
    {
        $repeatable = $this->getRepeatablePool($queue);
        $redis = Yii::app()->redis;

        $ids = $redis->zrangebyscore($repeatable->name, 0, time(), 'LIMIT', 0, 1);
        if (!$ids) return;
        
        $id = reset($ids);
        $repeatable->add($id, time() + $this->getJobInterval($id));

        if ($job = $this->getJob($id)) {
            $job['id'] = $id;
        }

        return $job;
    }

    /**
     * Pop schedlued or simple job id from given queue.
     * 
     * @param  string[optional] $queue Yiiq::DEFAULT_QUEUE by default
     * @return string
     */
    public function popJob($queue = self::DEFAULT_QUEUE)
    {
        $methods = array(
            'popScheduledJob', 
            'popRepeatableJob', 
            'popSimpleJob',
        );

        foreach ($methods as $method) {
            $id = $this->$method($queue);
            if ($id) return $id;
        }
    }

    /**
     * Remove dead process pids from redis set.
     * 
     * @param  ARedisSet $pool
     * @return int              amount of dead pids
     */
    public function checkPidPool(ARedisSet $pool)
    {
        $removed = 0;
        $pids = $pool->getData();
        foreach ($pids as $pid) {
            if ($this->isPidAlive($pid)) continue;
            $pool->remove($pid);
            $removed++;
        }

        return $removed;
    }

    /**
     * Check redis db consistency.
     * 
     * Remove dead worker pids from pid pool.
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
    }

}