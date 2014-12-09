<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains job result class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.jobs
 */

namespace Yiiq\jobs;

use Yiiq\Yiiq;
use Yiiq\base\JobComponent;

/**
 * Job result class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class Result extends JobComponent
{
    /**
     * Get redis job result key for given id.
     *
     * @return string
     */
    protected function getKey()
    {
        return $this->owner->prefix.':result:'.$this->id;
    }

    /**
     * Save job result to redis.
     * Empty results will not be saved.
     *
     * @param mixed $result
     */
    public function save($result)
    {
        if ($result === null) {
            return;
        }

        $key = $this->getKey();
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
    public function get($clear = false)
    {
        $key = $this->getKey();
        $raw = \Yii::app()->redis->getClient()->get($key);

        if ($raw && $clear) {
            $this->clear();
        }

        return $raw ? \CJSON::decode($raw) : null;
    }

    /**
     * Delete job result from redis.
     *
     * @param string $id
     */
    public function clear()
    {
        $key = $this->getKey();
        \Yii::app()->redis->getClient()->del($key);
    }
}
