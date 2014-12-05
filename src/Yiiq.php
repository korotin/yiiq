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

use Yiiq\util\Health;
use Yiiq\util\PoolCollection;
use Yiiq\jobs\Job;
use Yiiq\jobs\Producer;

/**
 * Yiiq component class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 *
 * @property-read Health $health
 * @property-read PoolCollection $pools
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
     * Health checker.
     *
     * @var Health
     */
    protected $health       = null;

    /**
     * Pool collection.
     *
     * @var PoolCollection
     */
    protected $pools       = null;

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
     * Get health checker component.
     *
     * @return Health
     */
    public function getHealth()
    {
        if ($this->health === null) {
            $this->health = new Health($this);
        }

        return $this->health;
    }

    /**
     * Get pool collection component.
     *
     * @return PoolCollection
     */
    public function getPools()
    {
        if ($this->pools === null) {
            $this->pools = new PoolCollection($this);
            $this->getPools()->
                addPool('pids', '\ARedisSet')->
                addPool('executing', '\ARedisSortedSet')->
                addPool('completed', '\ARedisSet')->
                addPool('failed', '\ARedisSet')->
                addPoolGroup('children', '\ARedisSet')->
                addPoolGroup(self::TYPE_SIMPLE, '\ARedisSet')->
                addPoolGroup(self::TYPE_SCHEDULED, '\ARedisSortedSet')->
                addPoolGroup(self::TYPE_REPEATABLE, '\ARedisSortedSet');
        }

        return $this->pools;
    }

    /**
     * Does job with given id exist.
     *
     * @param  string  $id
     * @return boolean
     */
    public function exists($id)
    {
        $key = Job::getKey($this, $id);

        return \Yii::app()->redis->getClient()->exists($key);
    }

    /**
     * Get job.
     * If job is not found, \CException will be raised.
     *
     * @param  string $id
     * @return Job
     */
    public function get($id)
    {
        return new Job($this, $id);
    }

    /**
     * Create new job.
     *
     * @return Job
     */
    protected function create()
    {
        return new Job($this);
    }

    /**
     * Delete job by id.
     * By default ($withData = true) job data will be also
     * deleted.
     *
     * @param string $id
     * @param bool   $withData (optional)
     */
    public function delete($id, $withData = true)
    {
        if (!$this->exists($id)) {
            return;
        }

        $job = $this->get($id);
        $this->getPools()->{$job->type}[$job->queue]->remove($id);

        if ($withData) {
            $job->delete();
        }
    }

    /**
     * Reenqueue job by id.
     *
     * @param  string $id
     * @return bool   true if job was restored
     */
    public function restore($id)
    {
        if (!$this->exists($id)) {
            return false;
        }

        $job = $this->get($id);

        $job->faults++;
        $job->lastFailed = time();

        $this->getPools()->executing->remove($id);

        if (
            $this->faultIntervals
            && $job->faults > count($this->faultIntervals)
        ) {
            $job->save(true);
            $this->delete($id, false);
            $this->getPools()->failed->add($id);

            return false;
        }

        $timestamp = $job->lastFailed + $this->faultIntervals[$job->faults - 1];

        switch ($job->type) {
            case self::TYPE_SIMPLE:
                $job->type = self::TYPE_SCHEDULED;
                $job->timestamp = $timestamp;
                // no break

            case self::TYPE_SCHEDULED:
                $this->getPools()->scheduled[$job->queue]->add($job->id, $timestamp);
                break;

            case self::TYPE_REPEATABLE:
                $this->getPools()->repeatable[$job->queue]->add($job->id, $timestamp);
                break;
        }

        $job->save(true);

        return true;
    }

    /**
     * Create a simple job in given queue.
     *
     * @param  string      $class job class extended from \Yiiq\jobs\Base
     * @param  array       $args  (optional) values for job object properties
     * @param  string      $queue (optional) \Yiiq\Yiiq::DEFAULT_QUEUE by default
     * @param  string|null $id    (optional) globally unique job id
     * @return Job|null
     */
    public function enqueue($class, array $args = [], $queue = self::DEFAULT_QUEUE, $id = null)
    {
        $job = $this->create();
        $job->id = $id;
        $job->queue = $queue;
        $job->type = self::TYPE_SIMPLE;
        $job->class = $class;
        $job->args = $args;

        if ($id = $job->save()) {
            $this->getPools()->simple[$queue]->add($id);
        }

        return $job;
    }

    /**
     * Create a job in given queue wich will be executed at given time.
     *
     * @param  integer     $timestamp timestamp to execute job at
     * @param  string      $class     job class extended from \Yiiq\jobs\Base
     * @param  array       $args      (optional) values for job object properties
     * @param  string      $queue     (optional) \Yiiq\Yiiq::DEFAULT_QUEUE by default
     * @param  string|null $id        (optional) globally unique job id
     * @return Job|null
     */
    public function enqueueAt($timestamp, $class, array $args = [], $queue = self::DEFAULT_QUEUE, $id = null)
    {
        $job = $this->create();
        $job->id = $id;
        $job->queue = $queue;
        $job->type = self::TYPE_SCHEDULED;
        $job->class = $class;
        $job->args = $args;
        $job->timestamp = $timestamp;

        if ($id = $job->save()) {
            $this->getPools()->scheduled[$queue]->add($id, $timestamp);
        }

        return $job;
    }

    /**
     * Create a job in given queue wich will be executed after specified interval.
     *
     * @param  integer     $interval time to wait before execution in seconds
     * @param  string      $class    job class extended from \Yiiq\jobs\Base
     * @param  array       $args     (optional) values for job object properties
     * @param  string      $queue    (optional) \Yiiq\Yiiq::DEFAULT_QUEUE by default
     * @param  string|null $id       (optional) globally unique job id
     * @return Job|null
     */
    public function enqueueAfter($interval, $class, array $args = [], $queue = self::DEFAULT_QUEUE, $id = null)
    {
        $interval = (int) floor($interval);

        return $this->enqueueAt(time() + $interval, $class, $args, $queue, $id);
    }

    /**
     * Create a job, which will execute each $interval seconds.
     *
     * Unlike other job types repeatable job requires $id to be set.
     * If job with given id already exists, it will be overwritten.
     *
     * @param  string  $id
     * @param  integer $interval
     * @param  string  $class
     * @param  array   $args
     * @param  string  $queue
     * @return Job
     */
    public function enqueueRepeatable($id, $interval, $class, array $args = [], $queue = self::DEFAULT_QUEUE)
    {
        $interval = (int) floor($interval);

        $job = $this->create();
        $job->id = $id;
        $job->queue = $queue;
        $job->type = self::TYPE_REPEATABLE;
        $job->class = $class;
        $job->args = $args;
        $job->interval = $interval;

        if ($id = $job->save()) {
            $this->getPools()->repeatable[$queue]->add($id, time());
        }

        return $job;
    }

    /**
     * Create a job via job producer.
     *
     * @param  string   $class
     * @return Producer
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
     * @param  Producer    $producer
     * @return string|null
     */
    public function enqueueByProducer(Producer $producer)
    {
        switch ($producer->type) {
            case self::TYPE_SIMPLE:
                return $this->enqueueJob(
                    $producer->class,
                    $producer->args,
                    $producer->queue,
                    $producer->id
                );
                // no break

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
                    return $this->enqueueJobAfter(
                        $producer->interval,
                        $producer->class,
                        $producer->args,
                        $producer->queue,
                        $producer->id
                    );
                }
                // no break

            case self::TYPE_REPEATABLE:
                return $this->enqueueRepeatableJob(
                    $producer->id,
                    $producer->interval,
                    $producer->class,
                    $producer->args,
                    $producer->queue
                );
                // no break
        }
    }

    /**
     * Pop job from sorted set.
     *
     * @param  \ARedisSortedSet $set
     * @return Job|null
     */
    protected function popFromSortedSet(\ARedisSortedSet $set)
    {
        $ids = \Yii::app()->redis->zrangebyscore(
            $set->name,
            0,
            time(),
            ['limit' => [0, 1]]
        );

        if (!$ids) {
            return;
        }

        return $this->get(reset($ids));
    }

    /**
     * Pop simple job id from given queue.
     *
     * @param  string   $queue
     * @return Job|null
     */
    protected function popSimpleJob($queue)
    {
        if (!($id = $this->getPools()->simple[$queue]->pop())) {
            return;
        }

        return $this->get($id);
    }

    /**
     * Pop scheduled job id from given queue.
     *
     * @param  string   $queue
     * @return Job|null
     */
    protected function popScheduledJob($queue)
    {
        $scheduled = $this->getPools()->scheduled[$queue];

        $data = $this->popFromSortedSet($scheduled);
        if (!$data) {
            return;
        }

        \Yii::app()->redis->zrem(
            $scheduled->name,
            $data->id
        );

        return $data;
    }

    /**
     * Pop repeatable job id from given queue.
     *
     * @param  string   $queue
     * @return Job|null
     */
    protected function popRepeatableJob($queue)
    {
        $repeatable = $this->getPools()->repeatable[$queue];

        $data = $this->popFromSortedSet($repeatable);
        if (!$data) {
            return;
        }

        $repeatable->add(
            $data->id,
            time() + $data->interval
        );

        return $data;
    }

    /**
     * Pop schedlued or simple job id from given queue.
     *
     * @param  string   $queue (optional) Yiiq::DEFAULT_QUEUE by default
     * @return Job|null
     */
    public function popJob($queue = self::DEFAULT_QUEUE)
    {
        $methods = [
            'popScheduledJob',
            'popRepeatableJob',
            'popSimpleJob',
        ];

        foreach ($methods as $method) {
            $jobJob = $this->$method($queue);
            if ($jobJob) {
                return $jobJob;
            }
        }
    }
}
