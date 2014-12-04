<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains tests for bad scheduled jobs.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.unit.jobs.scheduled
 */

namespace Yiiq\test\unit\jobs;

use Yiiq\test\cases\Job;

class BadTest extends Job
{
    /**
     * @dataProvider startParametersAndBadClassProvider
     */
    public function testBadScheduledJob($queue, $threads, $badClass)
    {
        $procTitle  = $this->getBaseProcessTitle();
        $logPath    = $this->getLogPath();
        $goodFile   = 'goodjob_'.TEST_TOKEN;
        $goodPath   = $this->getRuntimePath().'/'.$goodFile;

        $this->assertNotContains($procTitle, $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $this->assertFalse(file_exists($goodPath));
        \Yii::app()->yiiq->enqueueJobAfter(2, $badClass, [], $queue);

        usleep(3000000);
        $this->waitForJobs($threads, 1, true);

        $size = filesize($logPath);
        $this->assertGreaterThan(0, $size);
        $this->assertContains('YiiqTest', $this->exec('ps aux'));

        $this->assertFalse(file_exists($goodPath));
        \Yii::app()->yiiq->enqueueJobAfter(2, '\Yiiq\test\jobs\GoodJob', ['file' => $goodFile], $queue);

        usleep(3000000);
        $this->waitForJobs($threads, 1);

        $this->assertEquals($size, filesize($logPath));
        $this->assertTrue(file_exists($goodPath));

        $this->assertTrue(\Yii::app()->yiiq->health->check(false));
        $this->stopYiiq();
    }
}
