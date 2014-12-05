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
use Yiiq\base\Component;

/**
 * Job  class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class Job extends Component
{
    public $created = null;

    public $id = null;
    public $queue = null;
    public $type = null;
    public $class = null;
    public $args =  null;
    public $timestamp = null;
    public $interval = null;
    public $faults = 0;
    public $lastFailed = null;

    /**
     * Get redis job key for given id.
     *
     * @param  string $id
     * @return string
     */
    public static function getKey(Yiiq $owner, $id)
    {
        return $owner->prefix.':job:'.$id;
    }

    public function __construct(Yiiq $owner, $id = null)
    {
        parent::__construct($owner);

        if ($id) {
            $this->id = $id;
            $this->refresh();
        }
    }

    public function __toString()
    {
        $keys = [
            'created',
            'id',
            'queue',
            'type',
            'class',
            'args',
            'timestamp',
            'interval',
            'faults',
            'lastFailed',
        ];

        $data = array();
        foreach ($keys as $k) {
            $data[$k] = $this->$k;
        }
        $data = array_filter($data);

        return \CJSON::encode($data);
    }

    /**
     * Generate job id by increment.
     *
     * @return string
     */
    protected function generateId()
    {
        return \Yii::app()->redis->incr($this->owner->prefix.':counter');
    }

    /**
     * Save job data to redis.
     *
     * @param  boolean     $overwrite (optional) overwrite existing job, default is false
     * @return string|null id if job is saved or null
     */
    public function save($overwrite = false)
    {
        if (
            $this->id
            && $this->owner->exists($this->id)
            && !$overwrite
        ) {
            return;
        }

        $this->id = $this->id ?: $this->generateId();
        $this->created = time();

        $saved = \Yii::app()->redis->getClient()->set(
            self::getKey($this->owner, $this->id),
            (string) $this
        );

        if (!$saved) {
            return;
        }

        return $this->id;
    }

    public function delete()
    {
        \Yii::app()->redis->getClient()->del(
            self::getKey($this->owner, $this->id)
        );
    }

    /**
     * Is job executing at moment.
     *
     * @return boolean
     */
    public function isExecuting()
    {
        return \Yii::app()->redis->getClient()->zrank(
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
    public function isCompleted()
    {
        return (bool) \Yii::app()->redis->getClient()->sismember(
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
    public function isFailed()
    {
        return (bool) \Yii::app()->redis->getClient()->sismember(
            $this->owner->pools->failed->name,
            $this->id
        );
    }

    /**
     * Get redis job result key for given id.
     *
     * @return string
     */
    protected function getResultKey()
    {
        return $this->owner->prefix.':result:'.$this->id;
    }

    /**
     * Save job result to redis.
     * Empty results will not be saved.
     *
     * @param mixed $result
     */
    protected function saveResult($result)
    {
        if ($result === null) {
            return;
        }

        $key = $this->getResultKey();
        $result = \CJSON::encode($result);

        \Yii::app()->redis->getClient()->set(
            $key,
            $result
        );
    }

    /**
     * Get job result.
     *
     * @param  string  $id
     * @param  boolean $clear delete result from redis
     * @return mixed
     */
    public function getResult($clear = false)
    {
        $key = $this->getResultKey();
        $raw = \Yii::app()->redis->getClient()->get($key);

        if ($raw && $clear) {
            $this->clearResult();
        }

        return $raw ? \CJSON::decode($raw) : null;
    }

    /**
     * Delete job result from redis.
     *
     * @param string $id
     */
    public function clearResult()
    {
        $key = $this->getResultKey();
        \Yii::app()->redis->getClient()->del($key);
    }

    public function refresh()
    {
        if (!$this->id) {
            return;
        }

        $key = Job::getKey($this->owner, $this->id);
        $data = \Yii::app()->redis->getClient()->get($key);
        if (!$data) {
            return;
        }

        $data = \CJSON::decode($data);
        foreach ($data as $k => $v) {
            $this->$k = $v;
        }
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
     * Mark job as completed.
     * Jod id will be removed from executing pool and job data will be deleted
     * for non-repeatable jobs.
     *
     * @param mixed $result
     */
    public function markAsCompleted($result)
    {
        $this->owner->pools->executing->remove($this->id);
        if ($this->type === Yiiq::TYPE_REPEATABLE) {
            return;
        }

        $this->owner->pools->completed->add($this->id);
        $this->delete();

        $this->saveResult($result);
    }

    /**
     * Get job payload object.
     *
     * @return \Yiiq\jobs\Base
     */
    public function getPayload()
    {
        $class = $this->class;

        return new $class($this->queue, $this->type, $this->id);
    }
}
