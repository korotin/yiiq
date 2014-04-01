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
     * @param  string $class
     * @param  array[optional]  $args
     * @return string
     */
    protected function serializeJob($queue, $class, array $args= array())
    {
        return CJSON::encode(compact('queue', 'class', 'args'));
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
    protected function isPidAlive($pid)
    {
        $pids = null;
        exec('sh -c "ps ax | grep '.((int) $pid).' | grep -v grep"', $pids);
        return count($pids) > 0;
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
     * Get redis job pool for given queue.
     * Second parameter, $type, is not used for now.
     * 
     * @param  string[optional] $queue Yiiq::DEFAULT_QUEUE by default
     * @return ARedisSet
     */
    public function getQueue($queue = self::DEFAULT_QUEUE)
    {
        if (!$queue) {
            $queue = self::DEFAULT_QUEUE;
        }

        if (!isset($this->queues[$queue])) {
            $this->queues[$queue] = new ARedisSet($this->prefix.':queue:'.$queue.':'.self::TYPE_SIMPLE);
        }

        return $this->queues[$queue];
    }

    public function getSchedule($queue = self::DEFAULT_QUEUE)
    {
        if (!$queue) {
            $queue = self::DEFAULT_QUEUE;
        }

        if (!isset($this->schedules[$queue])) {
            $this->schedules[$queue] = new ARedisSortedSet($this->prefix.':queue:'.$queue.':'.self::TYPE_SCHEDULED);
        }

        return $this->schedules[$queue];
    }

    public function getRepeatable($queue = self::DEFAULT_QUEUE)
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
     * Does job with given id exists.
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
     * Delete job data by id.
     * 
     * @param  string $id
     */
    public function deleteJob($id)
    {
        $key = $this->getJobKey($id);
        Yii::app()->redis->getClient()->del($key);
    }

    protected function saveJob($queue, $id, $class, $args)
    {
        if ($id && $this->hasJob($id)) return null;
        
        $id     = $id ?: $this->generateJobId();
        $key    = $this->getJobKey($id);
        $job    = $this->serializeJob($queue, $class, $args);

        Yii::app()->redis->getClient()->set($key, $job);

        return $id;
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
        if ($id = $this->saveJob($queue, $id, $class, $args)) {
            $this->getQueue($queue)->add($id);
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
        if ($id = $this->saveJob($queue, $id, $class, $args)) {
            $this->getSchedule($queue)->add($id, $timestamp);
        }

        return $id;
    }

    /**
     * Create a job in given queue wich will be executed after given amount of seconds.
     * 
     * @param  int $seconds             amount of seconds
     * @param  string $class            job class extended from YiiqBaseJob
     * @param  array[optional] $args    values for job object properties
     * @param  string[optional] $queue  Yiiq::DEFAULT_QUEUE by default
     * @param  string[optional] $id     globally unique job id             
     * @return mixed                    job id or null if job with same id extists
     */
    public function enqueueJobIn($seconds, $class, array $args = array(), $queue = self::DEFAULT_QUEUE, $id = null)
    {
        if ($id = $this->saveJob($queue, $id, $class, $args)) {
            $this->getSchedule($queue)->add($id, time() + $seconds);
        }

        return $id;
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

    public function enqueueRepeatableJob($id, $interval, $class, array $args = array(), $queue = self::DEFAULT_QUEUE)
    {
        if ($id = $this->saveJob($queue, $id, $class, $args)) {
            $this->setJobInterval($id, $interval);
            $this->getRepeatable($queue)->add($id, time() + $seconds);
        }

        return $id;
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
        $schedule = $this->getSchedule($queue);
        $redis = Yii::app()->redis;

        $ids = $redis->zrangebyscore($schedule->name, 0, time(), 'LIMIT', 0, 1);
        if (!$ids) return;
        
        $id = reset($ids);
        $redis->zrem($schedule->name, $id);

        if ($job = $this->getJob($id)) {
            $job['id'] = $id;
            $job['type'] = self::TYPE_SCHEDULED;

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
        $repeatable = $this->getRepeatable($queue);
        $redis = Yii::app()->redis;

        $ids = $redis->zrangebyscore($repeatable->name, 0, time(), 'LIMIT', 0, 1);
        if (!$ids) return;
        
        $id = reset($ids);
        $repeatable->add($id, time() + $this->getJobInterval($id));

        if ($job = $this->getJob($id)) {
            $job['id'] = $id;
            $job['type'] = self::TYPE_REPEATABLE;
        }

        return $job;
    }

    /**
     * Pop simple job id from given queue.
     * 
     * @param  string $queue
     * @return mixed            job id or null
     */
    protected function popSimpleJob($queue)
    {
        if (!($id = $this->getQueue($queue)->pop())) return;

        if ($job = $this->getJob($id)) {
            $job['id'] = $id;
            $job['type'] = self::TYPE_SIMPLE;
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
     */
    public function checkPidPool(ARedisSet $pool)
    {
        $pids = $pool->getData();
        foreach ($pids as $pid) {
            if ($this->isPidAlive($pid)) continue;
            $pool->remove($pid);

        }
    }

    /**
     * Check redis db consistency.
     * 
     * Remove dead worker pids from pid pool.
     */
    public function check()
    {
        $this->checkPidPool($this->getPidPool());
    }

}