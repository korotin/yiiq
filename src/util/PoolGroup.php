<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains pool group class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.util
 */

namespace Yiiq\util;

use Yiiq\Yiiq;
use Yiiq\base\Component;

/**
 * Pool group class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class PoolGroup extends Component implements \ArrayAccess
{
    /**
     * Pool type.
     *
     * @var string
     */
    protected $type = null;

    /**
     * Pool class.
     *
     * @var string
     */
    protected $class = null;

    /**
     * Pool array.
     *
     * @var \ARedisIterableEntity
     */
    protected $pools = [];

    public function __construct(Yiiq $owner, $type, $class)
    {
        parent::__construct($owner);
        $this->type = $type;
        $this->class = $class;
    }

    public function offsetExists($offset)
    {
        return true;
    }

    public function offsetGet($offset)
    {
        if (!isset($this->pools[$offset])) {
            $class = $this->class;
            $this->pools[$offset] = new $class($this->owner->prefix.':'.$this->type.':'.$offset);
        }

        return $this->pools[$offset];
    }

    public function offsetSet($offset, $value)
    {
        throw new \CException(__CLASS__.' is a read-only storage.');
    }

    public function offsetUnset($offset)
    {
        throw new \CException(__CLASS__.' is a read-only storage.');
    }
}
