<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains queue class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.util
 */

namespace Yiiq\util;

use Yiiq\Yiiq;
use Yiiq\base\Component;
use Yiiq\jobs\Job;

/**
 * Queue class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class Queue extends Component
{
    /**
     * Queue name.
     * 
     * @var string
     */
    protected $name = null;

    public function __construct(Yiiq $owner, $name)
    {
        $this->owner = $owner;
        $this->name = $name;
    }

    public function __toString()
    {
        return $this->name;
    }

    /**
     * Add job to queue.
     * 
     * @param  Job    $job
     */
    public function push(Job $job)
    {
        $metadata = $job->metadata;
        if ($this->name !== $metadata->queue) {
            throw new \CException('Cannot push job with wrong queue name.');
        }

        $pool = $this->owner->pools->{$metadata->type}[$this->name];

        switch ($metadata->type) {
            case Yiiq::TYPE_SIMPLE:
                $pool->add($job->id);
                break;

            case Yiiq::TYPE_SCHEDULED:
                $pool->add($job->id, $metadata->timestamp);
                break;

            case Yiiq::TYPE_REPEATABLE:
                $pool->add($job->id, time());
                break;
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

        return $this->owner->get(reset($ids));
    }

    /**
     * Pop simple job id from given queue.
     *
     * @return Job|null
     */
    protected function popSimple()
    {
        if (!($id = $this->owner->pools->simple[$this->name]->pop())) {
            return;
        }

        return $this->owner->get($id);
    }

    /**
     * Pop scheduled job id from given queue.
     *
     * @return Job|null
     */
    protected function popScheduled()
    {
        $scheduled = $this->owner->pools->scheduled[$this->name];

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
     * @return Job|null
     */
    protected function popRepeatable()
    {
        $repeatable = $this->owner->pools->repeatable[$this->name];

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
     * @return Job|null
     */
    public function pop()
    {
        $methods = [
            'popScheduled',
            'popRepeatable',
            'popSimple',
        ];

        foreach ($methods as $method) {
            $job = $this->$method();
            if ($job) {
                return $job;
            }
        }
    }
}
