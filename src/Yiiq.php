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
use Yiiq\util\QueueCollection;
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
 * @property-read QueueCollection $queues
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
    protected $pools        = null;

    /**
     * Queue collection.
     *
     * @var QueueCollection
     */
    protected $queues       = null;

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
     * Get queue collection.
     *
     * @return QueueCollection
     */
    public function getQueues()
    {
        if ($this->queues === null) {
            $this->queues = new QueueCollection($this);
        }

        return $this->queues;
    }

    /**
     * Set process title to given string.
     * Process title is changing by cli_set_process_title (PHP >= 5.5) or
     * setproctitle (if proctitle extension is available).
     *
     * @param string          $type
     * @param string|string[] $queue
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
     * @param  string|null $class
     * @param  string|null $id
     * @return Builder
     */
    public function create($class = null, $id = null)
    {
        $builder = new Builder($this, $id ?: $this->generateId());
        if ($class) {
            $builder->withPayload($class);
        }

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

        $this->getQueues()[$job->metadata->queue]->delete($job);

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
     * Create base builder configuration.
     *
     * @param  string      $class job class extended from \Yiiq\jobs\Base
     * @param  array       $args  values for job object properties
     * @param  string      $queue \Yiiq\Yiiq::DEFAULT_QUEUE by default
     * @param  string|null $id    globally unique job id
     * @return Builder
     */
    protected function enqueueBase($class, array $args, $queue, $id)
    {
        return $this->
            create($class, $id)->
            into($queue)->
            withArgs($args);
    }

    /**
     * Create a simple job in given queue.
     *
     * If job with specified id exists, job will be not created and null will be returned.
     *
     * @param  string      $class job class extended from \Yiiq\jobs\Base
     * @param  array       $args  (optional) values for job object properties
     * @param  string      $queue (optional) \Yiiq\Yiiq::DEFAULT_QUEUE by default
     * @param  string|null $id    (optional) globally unique job id
     * @return Job|null
     */
    public function enqueue($class, array $args = [], $queue = self::DEFAULT_QUEUE, $id = null)
    {
        return $this->
            enqueueBase($class, $args, $queue, $id)->
            enqueue();
    }

    /**
     * Create a job in given queue wich will be executed at given time.
     *
     * If job with specified id exists, job will be not created and null will be returned.
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
            enqueueBase($class, $args, $queue, $id)->
            runAt($timestamp)->
            enqueue();
    }

    /**
     * Create a job in given queue wich will be executed after specified interval.
     *
     * If job with specified id exists, job will be not created and null will be returned.
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
            enqueueBase($class, $args, $queue, $id)->
            runAfter($interval)->
            enqueue();
    }

     /**
     * Create a job, which will execute each $interval seconds.
     *
     * If job with given id already exists, it will be overwritten.
     *
     * @param  integer     $interval interval between executions in seconds
     * @param  string      $class    job class extended from \Yiiq\jobs\Base
     * @param  array       $args     (optional) values for job object properties
     * @param  string      $queue    (optional) \Yiiq\Yiiq::DEFAULT_QUEUE by default
     * @param  string|null $id       (optional) globally unique job id
     * @return Job|null
     */
    public function enqueueEach($interval, $class, array $args = [], $queue = self::DEFAULT_QUEUE, $id = null)
    {
        return $this->
            enqueueBase($class, $args, $queue, $id)->
            runEach($interval)->
            enqueue();
    }
}
