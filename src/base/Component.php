<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains Yiiq's component class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.base
 */

namespace Yiiq\base;

use Yiiq\Yiiq;

/**
 * Yiiq's component class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
abstract class Component extends \CComponent
{
    /**
     * Yiiq component instance.
     *
     * @var \Yiiq\Yiiq
     */
    protected $owner    = null;

    public function __construct(Yiiq $owner)
    {
        $this->owner = $owner;
    }
}
