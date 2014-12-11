<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains Yiiq job builder class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.jobs
 */

namespace Yiiq\jobs;

use Yiiq\Yiiq;
use Yiiq\base\JobComponent;

/**
 * Yiiq job builder class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class Builder extends JobComponent
{
    /**
     * @var Metadata
     */
    protected $metadata = null;

    /**
     * @param string $id
     */
    public function __construct(Yiiq $owner, $id)
    {
        parent::__construct($owner, $id);

        $this->metadata = new Metadata($owner, $id);
    }

    /**
     * Set job payload class.
     *
     * @param  string  $class
     * @return Builder
     */
    public function withPayload($class)
    {
        $this->metadata->class = $class;

        return $this;
    }

    /**
     * Put job in specified queue.
     *
     * @param  string  $queue
     * @return Builder
     */
    public function into($queue)
    {
        $this->metadata->queue = $queue;

        return $this;
    }

    /**
     * Set job arguments to following array.
     *
     * @param  array   $args
     * @return Builder
     */
    public function withArgs(array $args)
    {
        $this->metadata->args = $args;

        return $this;
    }

    /**
     * Run job at given timestamp (converts job to scheduled).
     *
     * @param  int     $timestamp
     * @return Builder
     */
    public function runAt($timestamp)
    {
        $this->metadata->type = Yiiq::TYPE_SCHEDULED;
        $this->metadata->timestamp = $timestamp;

        return $this;
    }

    /**
     * Run job after given interval (converts job to scheduled).
     *
     * @param  int     $interval
     * @return Builder
     */
    public function runAfter($interval)
    {
        $this->metadata->type = Yiiq::TYPE_SCHEDULED;
        $this->metadata->timestamp = time() + floor($interval);

        return $this;
    }

    /**
     * Run job each $interval seconds (converts job to repeatable).
     *
     * @param  int     $interval
     * @return Builder
     */
    public function runEach($interval)
    {
        $this->metadata->type = Yiiq::TYPE_REPEATABLE;
        $this->metadata->interval = $interval;

        return $this;
    }

    /**
     * Build a job.
     * Null is returned if job cannot be created.
     *
     * @return Job|null
     */
    public function build()
    {
        if (!$this->metadata->type) {
            $this->metadata->type = Yiiq::TYPE_SIMPLE;
        }

        if (!$this->metadata->queue) {
            $this->metadata->queue = Yiiq::DEFAULT_QUEUE;
        }

        $overwrite = $this->metadata->type === Yiiq::TYPE_REPEATABLE;

        if (!$this->metadata->save($overwrite)) {
            return;
        }

        $job = new Job($this->owner, $this->id);
        $job->metadata = $this->metadata;

        return $job;
    }

    /**
     * Build and enqueue job.
     * Null is returned if job cannot be created.
     *
     * @return Job|null
     */
    public function enqueue()
    {
        $job = $this->build();
        if (!$job) {
            return;
        }

        $this->owner->queues[$job->metadata->queue]->push($job);

        return $job;
    }
}
