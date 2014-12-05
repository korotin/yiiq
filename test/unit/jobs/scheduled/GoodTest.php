<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains tests for good scheduled jobs.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.unit.jobs.scheduled
 */

namespace Yiiq\test\unit\jobs;

use Yiiq\test\cases\Job;

class GoodTest extends Job
{
    /**
     * @dataProvider startParametersProvider
     */
    public function testScheduledJob($queue, $threads)
    {
        $procTitle  = $this->getBaseProcessTitle();
        $goodAtFile = 'goodjob_at_'.TEST_TOKEN;
        $goodAtPath = $this->getRuntimePath().'/'.$goodAtFile;
        $goodAfterFile = 'goodjob_after_'.TEST_TOKEN;
        $goodAfterPath = $this->getRuntimePath().'/'.$goodAfterFile;

        $this->assertNotContains($procTitle, $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $this->assertFalse(file_exists($goodAtPath));
        $this->assertFalse(file_exists($goodAfterPath));
        $jobs = [];
        $jobs[] = \Yii::app()->yiiq->enqueueAt(
            time() + 2,
            '\Yiiq\test\jobs\GoodJob',
            ['file' => $goodAtFile],
            $queue
        );
        $jobs[] = \Yii::app()->yiiq->enqueueAfter(
            2,
            '\Yiiq\test\jobs\GoodJob',
            ['file' => $goodAfterFile],
            $queue
        );

        usleep(100000);

        foreach ($jobs as $job) {
            $this->assertTrue(\Yii::app()->yiiq->exists($job->id));
            $this->assertFalse($job->isExecuting());
            $this->assertFalse($job->isCompleted());
            $this->assertFalse($job->isFailed());
        }

        usleep(self::TIME_FOR_JOB + 2000000);
        $this->waitForJobs($threads, 2);

        $this->assertTrue(file_exists($goodAtPath));
        $this->assertTrue(file_exists($goodAfterPath));
        foreach ($jobs as $job) {
            $this->assertFalse(\Yii::app()->yiiq->exists($job->id));
            $this->assertTrue($job->isCompleted());
            $this->assertFalse($job->isExecuting());
            $this->assertFalse($job->isFailed());
        }

        $this->assertTrue(\Yii::app()->yiiq->health->check(false));
        $this->stopYiiq();
    }
}
