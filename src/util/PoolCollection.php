<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains  pool collection class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.util
 */

namespace Yiiq\util;

use Yiiq\Yiiq;
use Yiiq\base\Collection;

/**
 * Pool collection class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class PoolCollection extends Collection
{
    /**
     * Add single pool to collection.
     * 
     * @param string $type
     * @param string $class
     * @return  PoolCollection
     */
    public function addPool($type, $class)
    {
        return $this->add(
            $type,
            new $class($this->owner->prefix.':'.$type)
        );
    }

    /**
     * Add pool group to collection.
     * 
     * @param string $type
     * @param string $class
     * @return  PoolCollection
     */
    public function addPoolGroup($type, $class)
    {
        return $this->add(
            $type,
            new PoolGroup($this->owner, $type, $class)
        );
    }
}
