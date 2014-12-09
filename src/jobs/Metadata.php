<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains job meta data class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.jobs
 */

namespace Yiiq\jobs;

use Yiiq\Yiiq;
use Yiiq\base\JobComponent;

/**
 * Job meta data class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class Metadata extends JobComponent
{
    public $created      = null;
    public $queue        = null;
    public $type         = null;
    public $class        = null;
    public $args         =  null;
    public $timestamp    = null;
    public $interval     = null;
    public $faults       = 0;
    public $lastFailed   = null;

    /**
     * Get redis job metadata key for given id.
     *
     * @param  string $id
     * @return string
     */
    public static function getKey(Yiiq $owner, $id)
    {
        return $owner->prefix.':job:'.$id;
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

    public function refresh()
    {
        if (!$this->id) {
            return;
        }

        $key = self::getKey($this->owner, $this->id);
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
     * Save job data to redis.
     *
     * @param  boolean $overwrite (optional) overwrite existing job, default is false
     * @return boolean
     */
    public function save($overwrite = false)
    {
        $exists = $this->owner->exists($this->id);
        if (
            $this->id
            && $exists
            && !$overwrite
        ) {
            return false;
        }

        if (!$exists) {
            $this->created = time();
        }

        return \Yii::app()->redis->getClient()->set(
            self::getKey($this->owner, $this->id),
            (string) $this
        );
    }

    public function delete()
    {
        \Yii::app()->redis->getClient()->del(
            self::getKey($this->owner, $this->id)
        );
    }
}
