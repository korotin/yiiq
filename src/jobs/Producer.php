<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains Yiiq job data class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.jobs
 */

namespace Yiiq\jobs;

use Yiiq\Yiiq;
use Yiiq\base\Component;

/**
 * Yiiq job producer class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class Producer extends Component
{
    /**
     * Job type.
     *
     * @var string
     */
    public $type        = Yiiq::TYPE_SIMPLE;

    /**
     * Job queue.
     *
     * @var string
     */
    public $queue       = Yiiq::DEFAULT_QUEUE;

    /**
     * Job class.
     *
     * @var string
     */
    public $class       = null;

    /**
     * Job arguments.
     *
     * @var array
     */
    public $args        = [];

    /**
     * Job id.
     *
     * @var string
     */
    public $id          = null;

    /**
     * Timestamp for scheduled job.
     *
     * @var int
     */
    public $timestamp   = null;

    /**
     * Interval for scheduled or repeatable job.
     *
     * @var int
     */
    public $interval    = null;

    /**
     * Constructor.
     *
     * @param Yiiq   $owner
     * @param string $class
     */
    public function __construct(Yiiq $owner, $class)
    {
        parent::__construct($owner);
        $this->class = $class;
    }

    /**
     * Put job in given queue.
     *
     * @param  string   $queue
     * @return Producer
     */
    public function into($queue)
    {
        $this->queue = $queue;

        return $this;
    }

    /**
     * Set job arguments to following array.
     *
     * @param  array    $args
     * @return Producer
     */
    public function withArgs(array $args)
    {
        $this->args = $args;

        return $this;
    }

    /**
     * Set job id.
     *
     * @param  string   $id
     * @return Producer
     */
    public function withId($id)
    {
        $this->id = $id;

        return $this;
    }

    /**
     * Run job at given timestamp (converts job to scheduled).
     *
     * @param  int      $timestamp
     * @return Producer
     */
    public function runAt($timestamp)
    {
        $this->type = Yiiq::TYPE_SCHEDULED;
        $this->timestamp = $timestamp;

        return $this;
    }

    /**
     * Run job after given interval (converts job to scheduled).
     *
     * @param  int      $interval
     * @return Producer
     */
    public function runAfter($interval)
    {
        $this->type = Yiiq::TYPE_SCHEDULED;
        $this->interval = $interval;

        return $this;
    }

    /**
     * Run job each $interval seconds (converts job to repeatable).
     *
     * @param  int      $interval
     * @return Producer
     */
    public function runEach($interval)
    {
        $this->type = Yiiq::TYPE_REPEATABLE;
        $this->interval = $interval;

        return $this;
    }

    /**
     * Equeue job.
     *
     * Must be called after everything was set.
     * Returns job id.
     *
     * @return string|null
     */
    public function enqueue()
    {
        return $this->owner->enqueueJobByProducer($this);
    }
}
