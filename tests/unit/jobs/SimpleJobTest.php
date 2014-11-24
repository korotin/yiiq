<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains tests for simple jobs.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.unit.jobs
 */

namespace Yiiq\tests\unit\jobs;

use Yiiq\tests\cases\JobCase;

class SimpleJobTest extends JobCase
{
    /**
     * @dataProvider startParametersProvider
     */
    public function testSimpleJob($queue, $threads)
    {
        $this->startYiiq($queue, $threads);

        $this->assertFalse(file_exists(__DIR__.'/../../runtime/goodjob'));
        $id = \Yii::app()->yiiq->enqueueJob('\Yiiq\tests\jobs\GoodJob', [], $queue);
        $this->waitForJobs($threads, 1);
        echo file_get_contents(__DIR__.'/../../runtime/yiiq.log');
        $this->assertEquals(0, filesize(__DIR__.'/../../runtime/yiiq.log'));
        $this->assertTrue(file_exists(__DIR__.'/../../runtime/goodjob'));
        $this->assertTrue(\Yii::app()->yiiq->isCompleted($id));
        $this->assertFalse(\Yii::app()->yiiq->isFailed($id));
        $this->assertFalse(\Yii::app()->yiiq->isExecuting($id));

        $this->assertTrue(\Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    /**
     * @dataProvider startParametersProvider
     */
    public function testManySimpleJobsAfterStart($queue, $threads)
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $ids = array();
        for ($i = 1; $i < 20; $i++) {
            $this->assertFalse(file_exists(__DIR__.'/../../runtime/goodjob'.$i));
            $ids[$i] = \Yii::app()->yiiq->enqueueJob('\Yiiq\tests\jobs\GoodJob', ['file' => 'goodjob'.$i], $queue);
            $this->assertFalse(\Yii::app()->yiiq->isCompleted($ids[$i]));
            $this->assertFalse(\Yii::app()->yiiq->isFailed($ids[$i]));
        }

        $this->waitForJobs($threads, 20);

        echo file_get_contents(__DIR__.'/../../runtime/yiiq.log');
        $this->assertEquals(0, filesize(__DIR__.'/../../runtime/yiiq.log'));
        for ($i = 1; $i < 20; $i++) {
            $this->assertTrue(file_exists(__DIR__.'/../../runtime/goodjob'.$i));
            $this->assertTrue(\Yii::app()->yiiq->isCompleted($ids[$i]));
            $this->assertFalse(\Yii::app()->yiiq->isFailed($ids[$i]));
            $this->assertFalse(\Yii::app()->yiiq->isExecuting($ids[$i]));
        }

        $this->assertTrue(\Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    /**
     * @dataProvider startParametersProvider
     */
    public function testManySimpleJobsBeforeStart($queue, $threads)
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));

        $ids = [];
        for ($i = 1; $i < 20; $i++) {
            $this->assertFalse(file_exists(__DIR__.'/../../runtime/goodjob'.$i));
            $ids[$i] = \Yii::app()->yiiq->enqueueJob('\Yiiq\tests\jobs\GoodJob', ['file' => 'goodjob'.$i], $queue);
            $this->assertFalse(\Yii::app()->yiiq->isCompleted($ids[$i]));
            $this->assertFalse(\Yii::app()->yiiq->isFailed($ids[$i]));
        }

        $this->startYiiq($queue, $threads);

        $this->waitForJobs($threads, 20);
        
        $this->assertEquals(0, filesize(__DIR__.'/../../runtime/yiiq.log'));
        for ($i = 1; $i < 20; $i++) {
            $this->assertTrue(file_exists(__DIR__.'/../../runtime/goodjob'.$i));
            $this->assertTrue(\Yii::app()->yiiq->isCompleted($ids[$i]));
            $this->assertFalse(\Yii::app()->yiiq->isFailed($ids[$i]));
            $this->assertFalse(\Yii::app()->yiiq->isExecuting($ids[$i]));
        }

        $this->assertTrue(\Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    /**
     * @dataProvider startParametersAndBadClassProvider
     */
    public function testBadSimpleJob($queue, $threads, $badClass)
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $this->assertFalse(file_exists(__DIR__.'/../../runtime/goodjob'));
        $id = \Yii::app()->yiiq->enqueueJob($badClass, [], $queue);
        $this->waitForJobs($threads, 1, true);
        
        $size = filesize(__DIR__.'/../../runtime/yiiq.log');
        $this->assertGreaterThan(0, $size);
        $this->assertContains('YiiqTest', $this->exec('ps aux'));

        $this->assertFalse(\Yii::app()->yiiq->isCompleted($id));
        $this->assertFalse(\Yii::app()->yiiq->isExecuting($id));
        $this->assertTrue(\Yii::app()->yiiq->isFailed($id));

        $this->assertFalse(file_exists(__DIR__.'/../../runtime/goodjob'));
        \Yii::app()->yiiq->enqueueJob('\Yiiq\tests\jobs\GoodJob', [], $queue);
        $this->waitForJobs($threads, 1);
        $this->assertEquals($size, filesize(__DIR__.'/../../runtime/yiiq.log'));
        $this->assertTrue(file_exists(__DIR__.'/../../runtime/goodjob'));

        $this->assertTrue(\Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    /**
     * @dataProvider startParametersAndBadClassProvider
     */
    public function testManyBadSimpleJobs($queue, $threads, $badClass)
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $this->assertFalse(file_exists(__DIR__.'/../../runtime/goodjob'));
        $ids = [];
        for ($i = 0; $i < 10; $i++) {
            $ids[] = \Yii::app()->yiiq->enqueueJob($badClass, [], $queue);
        }
        $this->waitForJobs($threads, 10, true);
        
        $size = filesize(__DIR__.'/../../runtime/yiiq.log');
        $this->assertGreaterThan(0, $size);
        $this->assertContains('YiiqTest', $this->exec('ps aux'));

        foreach ($ids as $id) {
            $this->assertFalse(\Yii::app()->yiiq->isCompleted($id));
            $this->assertFalse(\Yii::app()->yiiq->isExecuting($id));
            $this->assertTrue(\Yii::app()->yiiq->isFailed($id));
        }

        $this->assertContains('YiiqTest', $this->exec('ps aux'));
        $this->assertFalse(file_exists(__DIR__.'/../../runtime/goodjob'));
        \Yii::app()->yiiq->enqueueJob('\Yiiq\tests\jobs\GoodJob', [], $queue);
        $this->waitForJobs($threads, 1);
        $this->assertEquals($size, filesize(__DIR__.'/../../runtime/yiiq.log'));
        $this->assertContains('YiiqTest', $this->exec('ps aux'));
        $this->assertTrue(file_exists(__DIR__.'/../../runtime/goodjob'));

        $this->assertTrue(\Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }
}