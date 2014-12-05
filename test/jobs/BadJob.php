<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains bad job class #1.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.jobs
 */

namespace Yiiq\test\jobs;

class BadJob extends \Yiiq\jobs\Payload
{
    public function run()
    {
        test();
    }
}
