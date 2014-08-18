<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains Yiiq job data class.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package ext.yiiq.models
 */

/**
 * Yiiq job data class.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class YiiqJobData
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

    public function __construct($data = null)
    {

        if (is_string($data)) {
            $data = CJSON::decode($data);
        }

        if (is_array($data)) {
            foreach ($data as $k => $v) {
                $this->$k = $v;
            }
        }
    }

    public function __toString()
    {
        $keys = array(
            'created',
            'id',
            'queue',
            'type',
            'class',
            'args',
            'timestamp',
            'interval',
            'faults',
        );

        $data = array();
        foreach ($keys as $k) {
            $data[$k] = $this->$k;
        }
        $data = array_filter($data);

        return CJSON::encode($data);
    }

}