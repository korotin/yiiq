<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains bad job class #1.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.jobs
 */

namespace Yiiq\tests\jobs;

class BadJob extends \Yiiq\jobs\Base
{

    public function run()
    {
        test();
    }

}