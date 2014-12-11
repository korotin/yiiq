<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains base collection class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.base
 */

namespace Yiiq\base;

use Yiiq\Yiiq;

/**
 * Base collection class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
abstract class Collection extends Component implements \ArrayAccess
{
    /**
     * Items stored in collection.
     * 
     * @var array
     */
    protected $items = [];

    /**
     * Add item to collection.
     * Cannot be called outside class.
     * 
     * @param string $name
     * @param string $value
     */
    protected function add($name, $value)
    {
        $this->items[$name] = $value;

        return $this;
    }

    /**
     * This method will be executed before getting an element.
     * May be overriden in child classes.
     * 
     * @param  string $name 
     */
    protected function beforeGet($name)
    {
        return;
    }

    /**
     * Does a collection have an element with specified name.
     * 
     * @param  string  $name
     * @return boolean
     */
    public function has($name)
    {
        return isset($this->items[$name]);
    }

    public function get($name)
    {
        $this->beforeGet($name);

        if ($this->has($name)) {
            return $this->items[$name];
        }

        throw new \CException('Item '.$name.' is missing in '.__CLASS__.'.');
    }

    public function __get($name)
    {
        return $this->get($name);
    }

    public function offsetExists($offset)
    {
        $this->beforeGet($offset);
        return $this->has($offset);
    }

    public function offsetGet($offset)
    {
        return $this->get($offset);
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
