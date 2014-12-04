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
use Yiiq\base\Component;

/**
 * Pool collection class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class PoolCollection extends Component
{
    /**
     * @var array
     */
    protected $pools = [];

    public function addPool($type, $class)
    {
        $this->pools[$type] = new $class($this->owner->prefix.':'.$type);

        return $this;
    }

    public function addPoolGroup($type, $class)
    {
        $this->pools[$type] = new PoolGroup($this->owner, $type, $class);

        return $this;
    }

    public function __get($name)
    {
        if (isset($this->pools[$name])) {
            return $this->pools[$name];
        }

        return parent::__get($name);
    }
}
