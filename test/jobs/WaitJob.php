<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains wait job class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.jobs
 */

namespace Yiiq\test\jobs;

class WaitJob extends \Yiiq\jobs\Base
{
    public $sleep;

    public function run()
    {
        sleep($this->sleep);
    }
}
