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
    /**
     * Get string value.
     *
     * @return string
     */
    public function get()
    {
        return $this->getConnection()->get($this->name);
    }

    /**
     * Set string value.
     *
     * @param string $value
     */
    public function set($value)
    {
        $this->getConnection()->set($this->name, $value);
    }

    /**
     * Delete string.
     */
    public function del()
    {
        $this->getConnection()->del($this->name);
    }
}
