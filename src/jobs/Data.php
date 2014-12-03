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

/**
 * Yiiq job data class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class Data
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

    public function __construct($data = null)
    {

        if (is_string($data)) {
            $data = \CJSON::decode($data);
        }

        if (is_array($data)) {
            foreach ($data as $k => $v) {
                $this->$k = $v;
            }
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
}
