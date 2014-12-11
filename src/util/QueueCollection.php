<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains  queue collection class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.util
 */

namespace Yiiq\util;

use Yiiq\Yiiq;
use Yiiq\base\Collection;

/**
 * Queue collection class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class QueueCollection extends Collection
{
    protected function beforeGet($name)
    {
        if (!$this->has($name)) {
            $this->add($name, new Queue($this->owner, $name));
        }
    }
}
