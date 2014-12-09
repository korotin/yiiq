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
use Yiiq\jobs\Builder;
use Yiiq\jobs\Metadata;
use Yiiq\jobs\Job;

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
            $this->pools->
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
     * Set process title to given string.
     * Process title is changing by cli_set_process_title (PHP >= 5.5) or
     * setproctitle (if proctitle extension is available).
     *
     * @param string          $type
     * @param string $queue
     * @param string          $title
     */
    public function setProcessTitle($type, $queue, $title)
    {
        $titleTemplate = $this->titleTemplate;
        if (!$titleTemplate) {
            return;
        }

        if (is_array($queue)) {
            $queue = implode(',', $queue);
        }

        $placeholders = array(
            '{name}'    => $this->name,
            '{type}'    => $type,
            '{queue}'   => $queue,
            '{message}' => $title,
        );

        $title =
            str_replace(
                array_keys($placeholders),
                array_values($placeholders),
                $titleTemplate
            );

        if (function_exists('cli_set_process_title')) {
            cli_set_process_title($title);
        } elseif (function_exists('setproctitle')) {
            setproctitle($title);
        }
    }

    /**
     * Generate job id by increment.
     *
     * @return string
     */
    protected function generateId()
    {
        return \Yii::app()->redis->incr($this->prefix.':counter');
    }

    /**
     * Does job with given id exist.
     *
     * @param  string  $id
     * @return boolean
     */
    public function exists($id)
    {
        $key = Metadata::getKey($this, $id);

        return \Yii::app()->redis->getClient()->exists($key);
    }

    /**
     * Create a builder.
     *
     * @param  string      $class
     * @param  string|null $id
     * @return Builder
     */
    public function create($class, $id = null)
    {
        $builder = new Builder($this, $id ?: $this->generateId());
        $builder->withPayload($class);

        return $builder;
    }

    /**
     * Get job.
     *
     * @param  string|null $id
     * @return Job
     */
    public function get($id)
    {
        return new Job($this, $id);
    }

    /**
     * Delete job by id.
     * By default ($withMetadata = true) job data will be also
     * deleted.
     *
     * @param string $id
     * @param bool   $withMetadata (optional)
     */
    public function delete($id, $withMetadata = true)
    {
        if (!$this->exists($id)) {
            return;
        }

        $job = $this->get($id);
        $this->getPools()->{$job->metadata->type}[$job->metadata->queue]->remove($id);

        if ($withMetadata) {
            $job->metadata->delete();
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
        $metadata = $job->metadata;

        $metadata->faults++;
        $metadata->lastFailed = time();

        $this->getPools()->executing->remove($id);

        if (
            $this->faultIntervals
            && $metadata->faults > count($this->faultIntervals)
        ) {
            $metadata->save(true);
            $this->delete($id, false);
            $this->getPools()->failed->add($id);

            return false;
        }

        $timestamp = $metadata->lastFailed + $this->faultIntervals[$metadata->faults - 1];

        if ($metadata->type === self::TYPE_SIMPLE) {
            $metadata->type = self::TYPE_SCHEDULED;
            $metadata->timestamp = $timestamp;
        }

        $metadata->save(true);

        $this->getPools()->{$metadata->type}[$metadata->queue]->add($job->id, $timestamp);

        return true;
    }

    /**
     * Enqueue job.
     *
     * @param Job $job
     */
    public function enqueue(Job $job)
    {
        $metadata = $job->metadata;
        $pool = $this->getPools()->{$metadata->type}[$metadata->queue];

        switch ($metadata->type) {
            case self::TYPE_SIMPLE:
                $pool->add($job->id);
                break;

            case self::TYPE_SCHEDULED:
                $pool->add($job->id, $metadata->timestamp);
                break;

            case self::TYPE_REPEATABLE:
                $pool->add($job->id, time());
                break;
        }
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
    public function enqueueSimple($class, array $args = [], $queue = self::DEFAULT_QUEUE, $id = null)
    {
        return $this->
            create($class, $id)->
            into($queue)->
            withArgs($args)->
            enqueue();
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
        return $this->
            create($class, $id)->
            into($queue)->
            withArgs($args)->
            runAt($timestamp)->
            enqueue();
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
        return $this->
            create($class, $id)->
            into($queue)->
            withArgs($args)->
            runAfter($interval)->
            enqueue();
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
        return $this->
            create($class, $id)->
            into($queue)->
            withArgs($args)->
            runEach($interval)->
            enqueue();
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

        $job = $this->popFromSortedSet($scheduled);
        if (!$job) {
            return;
        }

        \Yii::app()->redis->zrem(
            $scheduled->name,
            $job->id
        );

        return $job;
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

        $job = $this->popFromSortedSet($repeatable);
        if (!$job) {
            return;
        }

        $repeatable->add(
            $job->id,
            time() + $job->metadata->interval
        );

        return $job;
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
