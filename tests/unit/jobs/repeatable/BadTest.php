<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains tests for bad repeatable jobs.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.unit.jobs.repeatable
 */

namespace Yiiq\tests\unit\jobs\repeatable;

use Yiiq\tests\cases\Job;

class BadTest extends Job
{
    /**
     * @dataProvider startParametersAndBadClassProvider
     */
    public function testBadRepeatableJob($queue, $threads, $badClass)
    {
        $procTitle  = $this->getBaseProcessTitle();
        $logPath    = $this->getLogPath();

        $this->assertNotContains($procTitle, $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        \Yii::app()->yiiq->enqueueRepeatableJob('badjob', 1, $badClass, [], $queue);

        $this->waitForJobs($threads, 1, true);

        $size = filesize($logPath);
        $this->assertGreaterThan(0, $size);
        $this->assertContains($procTitle, $this->exec('ps aux'));

        $this->assertTrue(\Yii::app()->yiiq->hasJob('badjob'));
        $this->assertFalse(\Yii::app()->yiiq->isCompleted('badjob'));
        $this->assertFalse(\Yii::app()->yiiq->isExecuting('badjob'));
        $this->assertTrue(\Yii::app()->yiiq->isFailed('badjob'));

        $this->assertTrue(\Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }
}