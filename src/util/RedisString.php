<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains redis string class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.util
 */

namespace Yiiq\util;

/**
 * Redis string class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class RedisString extends \ARedisEntity
{
    public function get()
    {
        return $this->getConnection()->get($this->name);
    }

    public function set($value)
    {
        $this->getConnection()->set($this->name, $value);
    }

    public function del()
    {
        $this->getConnection()->del($this->name);
    }
}
