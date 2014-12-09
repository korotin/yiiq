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

        $job = \Yii::app()->yiiq->enqueueSimple('\Yiiq\test\jobs\WaitJob', ['sleep' => 2], $queue);

        usleep(1500000);

        $this->assertFalse($job->status->isFailed);
        $this->assertFalse($job->status->isCompleted);
        $this->assertTrue($job->status->isExecuting);

        usleep(2500000);

        $this->assertFalse($job->status->isFailed);
        $this->assertTrue($job->status->isCompleted);
        $this->assertFalse($job->status->isExecuting);

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
        $job = \Yii::app()->yiiq->enqueueSimple('\Yiiq\test\jobs\ReturnJob', ['result' => $result], $queue);

        $this->waitForJobs($threads, 1);

        $size = filesize($this->getLogPath());
        $this->assertEquals(0, $size);
        $this->assertFalse($job->status->isFailed);
        $this->assertTrue($job->status->isCompleted);
        $this->assertFalse($job->status->isExecuting);
        $this->assertEquals($result, $job->result->get());
        $this->assertEquals($result, $job->result->get(true));
        $this->assertNull($job->result->get());

        $this->stopYiiq();
    }

    /**
     * @dataProvider queuesThreadsJobsProvider
     */
    public function testMultipleQueues($queuesCount, $threads, $jobCount)
    {
        $runtimePath = $this->getRuntimePath();

        $this->assertNotContains($this->getBaseProcessTitle(), $this->exec('ps aux'));
        $queues = [];
        for ($i = 0; $i < $queuesCount; $i++) {
            $queues[] = 'queue_'.md5(rand());
        }

        $this->startYiiq($queues, $threads);

        $jobs = [];
        $files = [];
        foreach ($queues as $index => $queue) {
            for ($i = 0; $i < $jobCount; $i++) {
                $files[] = 'goodjob_'.$queue.'_'.$i.'_'.TEST_TOKEN;
                $jobs[] =
                    \Yii::app()->yiiq->enqueueSimple(
                        '\Yiiq\test\jobs\GoodJob',
                        ['file' => $files[count($jobs)]],
                        $queue
                    );
            }
        }

        $this->waitForJobs($threads, count($jobs));

        $size = filesize($this->getLogPath());
        $this->assertEquals(0, $size);

        foreach ($jobs as $index => $job) {
            $this->assertFalse($job->status->isFailed);
            $this->assertTrue($job->status->isCompleted);
            $this->assertFalse($job->status->isExecuting);
            $this->assertTrue(file_exists($runtimePath.'/'.$files[$index]));
        }

        $this->stopYiiq();
    }
}
