<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains tests for bad repeatable jobs.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.unit.jobs.repeatable
 */

namespace Yiiq\test\unit\jobs\repeatable;

use Yiiq\test\cases\Job;

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

        $job = \Yii::app()->yiiq->enqueueRepeatable('badjob', 1, $badClass, [], $queue);

        $this->waitForJobs($threads, 1, true);

        $size = filesize($logPath);
        $this->assertGreaterThan(0, $size);
        $this->assertContains($procTitle, $this->exec('ps aux'));

        $this->assertTrue(\Yii::app()->yiiq->exists('badjob'));
        $this->assertFalse($job->status->isCompleted);
        $this->assertFalse($job->status->isExecuting);
        $this->assertTrue($job->status->isFailed);

        $this->assertTrue(\Yii::app()->yiiq->health->check(false));
        $this->stopYiiq();
    }
}
