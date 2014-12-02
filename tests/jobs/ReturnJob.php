<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains returnable job class.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.jobs
 */

namespace Yiiq\tests\jobs;

class ReturnJob extends \Yiiq\jobs\Base
{
    public $result;

    public function run()
    {
        return $this->result;
    }
}
