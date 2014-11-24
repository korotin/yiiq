<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains tests for scheduled jobs.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.unit.jobs
 */

namespace Yiiq\tests\unit\jobs;

use Yiiq\tests\cases\JobCase;

class ScheduledJobTest extends JobCase
{
    /**
     * @dataProvider startParametersProvider
     */
    public function testScheduledJob($queue, $threads)
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $this->assertFalse(file_exists(__DIR__.'/../../runtime/goodjob_at'));
        $this->assertFalse(file_exists(__DIR__.'/../../runtime/goodjob_in'));
        $ids = [];
        $ids[] = \Yii::app()->yiiq->enqueueJobAt(time() + 2, '\Yiiq\tests\jobs\GoodJob', array('file' => 'goodjob_at'), $queue);
        $ids[] = \Yii::app()->yiiq->enqueueJobIn(2, '\Yiiq\tests\jobs\GoodJob', array('file' => 'goodjob_in'), $queue);

        usleep(500000);

        foreach ($ids as $id) {
            $this->assertTrue(\Yii::app()->yiiq->hasJob($id));
            $this->assertFalse(\Yii::app()->yiiq->isExecuting($id));
            $this->assertFalse(\Yii::app()->yiiq->isCompleted($id));
            $this->assertFalse(\Yii::app()->yiiq->isFailed($id));
        }

        usleep(1500000);
        $this->waitForJobs($threads, 2);

        $this->assertTrue(file_exists(__DIR__.'/../../runtime/goodjob_at'));
        $this->assertTrue(file_exists(__DIR__.'/../../runtime/goodjob_in'));
        foreach ($ids as $id) {
            $this->assertFalse(\Yii::app()->yiiq->hasJob($id));
            $this->assertTrue(\Yii::app()->yiiq->isCompleted($id));
            $this->assertFalse(\Yii::app()->yiiq->isExecuting($id));
            $this->assertFalse(\Yii::app()->yiiq->isFailed($id));
        }

        $this->assertTrue(\Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    /**
     * @dataProvider startParametersAndBadClassProvider
     */
    public function testBadScheduledJob($queue, $threads, $badClass)
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $this->assertFalse(file_exists(__DIR__.'/../../runtime/goodjob'));
        \Yii::app()->yiiq->enqueueJobIn(2, $badClass, [], $queue);
        
        usleep(3000000);
        $this->waitForJobs($threads, 1, true);

        $size = filesize(__DIR__.'/../../runtime/yiiq.log');
        $this->assertGreaterThan(0, $size);
        $this->assertContains('YiiqTest', $this->exec('ps aux'));

        $this->assertFalse(file_exists(__DIR__.'/../../runtime/goodjob'));
        \Yii::app()->yiiq->enqueueJobIn(2, '\Yiiq\tests\jobs\GoodJob', [], $queue);
        
        usleep(3000000);
        $this->waitForJobs($threads, 1);

        $this->assertEquals($size, filesize(__DIR__.'/../../runtime/yiiq.log'));
        $this->assertTrue(file_exists(__DIR__.'/../../runtime/goodjob'));

        $this->assertTrue(\Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }
}