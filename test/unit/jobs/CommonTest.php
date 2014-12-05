<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains common tests for jobs.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.unit.jobs
 */

namespace Yiiq\test\unit\jobs;

use Yiiq\test\cases\Job;

class CommonTest extends Job
{
    /**
     * @dataProvider startParametersProvider
     */
    public function testJobStates($queue, $threads)
    {
        $this->assertNotContains($this->getBaseProcessTitle(), $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $job = \Yii::app()->yiiq->enqueue('\Yiiq\test\jobs\WaitJob', ['sleep' => 2], $queue);

        usleep(1500000);

        $this->assertFalse($job->isFailed());
        $this->assertFalse($job->isCompleted());
        $this->assertTrue($job->isExecuting());

        usleep(2500000);

        $this->assertFalse($job->isFailed());
        $this->assertTrue($job->isCompleted());
        $this->assertFalse($job->isExecuting());

        $this->stopYiiq();
    }

    /**
     * @dataProvider startParametersProvider
     */
    public function testResultSaving($queue, $threads)
    {
        $this->assertNotContains($this->getBaseProcessTitle(), $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $result = rand();
        $job = \Yii::app()->yiiq->enqueue('\Yiiq\test\jobs\ReturnJob', ['result' => $result], $queue);

        $this->waitForJobs($threads, 1);

        $size = filesize($this->getLogPath());
        $this->assertEquals(0, $size);
        $this->assertFalse($job->isFailed());
        $this->assertTrue($job->isCompleted());
        $this->assertFalse($job->isExecuting());
        $this->assertEquals($result, $job->getResult());
        $this->assertEquals($result, $job->getResult(true));
        $this->assertNull($job->getResult());

        $this->stopYiiq();
    }
}
