<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains job class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.jobs
 */

namespace Yiiq\jobs;

use Yiiq\Yiiq;
use Yiiq\base\JobComponent;

/**
 * Job status class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 *
 * @property-read boolean isExecuting
 * @property-read boolean isCompleted
 * @property-read boolean isFailed
 */
class Status extends JobComponent
{
    /**
     * Is job executing at moment.
     *
     * @return boolean
     */
    public function getIsExecuting()
    {
        return \Yii::app()->redis->client->zrank(
            $this->owner->pools->executing->name,
            $this->id
        ) !== false;
    }

    /**
     * Is job completed successfully.
     *
     * @param  string  $id
     * @return boolean
     */
    public function getIsCompleted()
    {
        return (bool) \Yii::app()->redis->client->sismember(
            $this->owner->pools->completed->name,
            $this->id
        );
    }

    /**
     * Is job failed.
     *
     * @param  string  $id
     * @return boolean
     */
    public function getIsFailed()
    {
        return (bool) \Yii::app()->redis->client->sismember(
            $this->owner->pools->failed->name,
            $this->id
        );
    }

    /**
     * Mark job as started.
     * Job id will be added to executing pool for current queue.
     *
     * @param int $pid forked worker pid
     */
    public function markAsStarted($pid)
    {
        $this->owner->pools->executing->add($this->id, $pid);
    }

    /**
     * Mark job as stopped.
     * Jod id will be removed from executing pool.
     *
     * @param mixed $result
     */
    public function markAsStopped()
    {
        $this->owner->pools->executing->remove($this->id);
    }

    /**
     * Mark job as completed.
     * Jod id will be removed from executing pool and added to completed pool.
     *
     * @param mixed $result
     */
    public function markAsCompleted()
    {
        $this->owner->pools->executing->remove($this->id);
        $this->owner->pools->completed->add($this->id);
    }
}
