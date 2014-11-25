<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains tests for repeatable jobs.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.unit.jobs
 */

namespace Yiiq\tests\unit\jobs;

use Yiiq\tests\cases\JobCase;

class RepeatableJobTest extends JobCase
{
    /**
     * @dataProvider startParametersProvider
     */
    public function testRepeatableJob($queue, $threads)
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        \Yii::app()->yiiq->enqueueRepeatableJob('goodjob', 1, '\Yiiq\tests\jobs\GoodJob', ['content' => '*'], $queue);

        usleep(5000000);

        $size = filesize(__DIR__.'/../../runtime/yiiq.log');
        $this->assertEquals(0, $size);
        $contentSize = filesize(__DIR__.'/../../runtime/goodjob');
        $this->assertGreaterThanOrEqual(4, $contentSize);
        $this->assertLessThanOrEqual(6, $contentSize);

        $this->assertTrue(\Yii::app()->yiiq->hasJob('goodjob'));
        $this->assertFalse(\Yii::app()->yiiq->isCompleted('goodjob'));
        $this->assertFalse(\Yii::app()->yiiq->isFailed('goodjob'));

        \Yii::app()->yiiq->deleteJob('goodjob');
        $this->assertFalse(\Yii::app()->yiiq->hasJob('goodjob'));

        usleep(200000);

        unlink(__DIR__.'/../../runtime/goodjob');

        usleep(600000);
        
        $this->assertFalse(file_exists(__DIR__.'/../../runtime/goodjob'));

        $this->assertTrue(\Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    /**
     * @dataProvider startParametersAndBadClassProvider
     */
    public function testBadRepeatableJob($queue, $threads, $badClass)
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        \Yii::app()->yiiq->enqueueRepeatableJob('badjob', 1, $badClass, [], $queue);

        $this->waitForJobs($threads, 1, true);

        $size = filesize(__DIR__.'/../../runtime/yiiq.log');
        $this->assertGreaterThan(0, $size);
        $this->assertContains('YiiqTest', $this->exec('ps aux'));

        $this->assertTrue(\Yii::app()->yiiq->hasJob('badjob'));
        $this->assertFalse(\Yii::app()->yiiq->isCompleted('badjob'));
        $this->assertFalse(\Yii::app()->yiiq->isExecuting('badjob'));
        $this->assertTrue(\Yii::app()->yiiq->isFailed('badjob'));

        $this->assertTrue(\Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }
}