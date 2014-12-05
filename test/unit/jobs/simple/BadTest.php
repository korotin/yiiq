<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains tests for bad simple jobs.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.unit.jobs.simple
 */

namespace Yiiq\test\unit\jobs\simple;

use Yiiq\test\cases\Job;

class BadTest extends Job
{
    /**
     * @dataProvider startParametersAndBadClassProvider
     */
    public function testBadSimpleJob($queue, $threads, $badClass)
    {
        $procTitle  = $this->getBaseProcessTitle();
        $logPath    = $this->getLogPath();
        $goodFile   = 'goodjob_'.TEST_TOKEN;
        $goodPath   = $this->getRuntimePath().'/'.$goodFile;

        $this->assertNotContains($procTitle, $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $this->assertFalse(file_exists($goodPath));
        $job = \Yii::app()->yiiq->enqueue($badClass, [], $queue);
        $this->waitForJobs($threads, 1, true);

        $size = filesize($logPath);
        $this->assertGreaterThan(0, $size);
        $this->assertContains($procTitle, $this->exec('ps aux'));

        $this->assertFalse($job->isCompleted());
        $this->assertFalse($job->isExecuting());
        $this->assertTrue($job->isFailed());

        $this->assertFalse(file_exists($goodPath));
        \Yii::app()->yiiq->enqueue('\Yiiq\test\jobs\GoodJob', ['file' => $goodFile], $queue);
        $this->waitForJobs($threads, 1);
        $this->assertEquals($size, filesize($logPath));
        $this->assertTrue(file_exists($goodPath));

        $this->assertTrue(\Yii::app()->yiiq->health->check(false));
        $this->stopYiiq();
    }

    /**
     * @dataProvider startParametersAndBadClassProvider
     */
    public function testManyBadSimpleJobs($queue, $threads, $badClass)
    {
        $procTitle  = $this->getBaseProcessTitle();
        $logPath    = $this->getLogPath();
        $goodFile   = 'goodjob_'.TEST_TOKEN;
        $goodPath   = $this->getRuntimePath().'/'.$goodFile;

        $this->assertNotContains($procTitle, $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $this->assertFalse(file_exists($goodPath));
        $jobs = [];
        for ($i = 0; $i < 10; $i++) {
            $jobs[] = \Yii::app()->yiiq->enqueue($badClass, [], $queue);
        }
        $this->waitForJobs($threads, 10, true);

        $size = filesize($logPath);
        $this->assertGreaterThan(0, $size);
        $this->assertContains($procTitle, $this->exec('ps aux'));

        foreach ($jobs as $job) {
            $this->assertFalse($job->isCompleted());
            $this->assertFalse($job->isExecuting());
            $this->assertTrue($job->isFailed());
        }

        $this->assertContains($procTitle, $this->exec('ps aux'));
        $this->assertFalse(file_exists($goodPath));
        \Yii::app()->yiiq->enqueue('\Yiiq\test\jobs\GoodJob', ['file' => $goodFile], $queue);

        $this->waitForJobs($threads, 1);

        $this->assertEquals($size, filesize($logPath));
        $this->assertContains($procTitle, $this->exec('ps aux'));
        $this->assertTrue(file_exists($goodPath));

        $this->assertTrue(\Yii::app()->yiiq->health->check(false));
        $this->stopYiiq();
    }
}
