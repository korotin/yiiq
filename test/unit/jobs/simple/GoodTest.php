<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains tests for good simple jobs.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.unit.jobs.simple
 */

namespace Yiiq\test\unit\jobs\simple;

use Yiiq\test\cases\Job;

class GoodTest extends Job
{
    /**
     * @dataProvider startParametersProvider
     */
    public function testSimpleJob($queue, $threads)
    {
        $procTitle  = $this->getBaseProcessTitle();
        $logPath    = $this->getLogPath();
        $goodFile   = 'goodjob_'.TEST_TOKEN;
        $goodPath   = $this->getRuntimePath().'/'.$goodFile;

        $this->startYiiq($queue, $threads);

        $this->assertFalse(file_exists($goodPath));
        $id = \Yii::app()->yiiq->enqueueJob('\Yiiq\test\jobs\GoodJob', ['file' => $goodFile], $queue);

        $this->waitForJobs($threads, 1);

        $this->assertEquals(0, filesize($logPath));
        $this->assertTrue(file_exists($goodPath));
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
        $procTitle  = $this->getBaseProcessTitle();
        $logPath    = $this->getLogPath();
        $goodFile   = 'goodjob_'.TEST_TOKEN.'_';
        $goodPath   = $this->getRuntimePath().'/'.$goodFile;

        $this->assertNotContains($procTitle, $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $ids = array();
        for ($i = 1; $i < 20; $i++) {
            $this->assertFalse(file_exists($goodPath.$i));
            $ids[$i] = \Yii::app()->yiiq->enqueueJob('\Yiiq\test\jobs\GoodJob', ['file' => $goodFile.$i], $queue);
            $this->assertFalse(\Yii::app()->yiiq->isCompleted($ids[$i]));
            $this->assertFalse(\Yii::app()->yiiq->isFailed($ids[$i]));
        }

        $this->waitForJobs($threads, 20);

        $this->assertEquals(0, filesize($logPath));
        for ($i = 1; $i < 20; $i++) {
            $this->assertTrue(file_exists($goodPath.$i));
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
        $procTitle  = $this->getBaseProcessTitle();
        $logPath    = $this->getLogPath();
        $goodFile   = 'goodjob_'.TEST_TOKEN.'_';
        $goodPath   = $this->getRuntimePath().'/'.$goodFile;

        $this->assertNotContains($procTitle, $this->exec('ps aux'));

        $ids = [];
        for ($i = 1; $i < 20; $i++) {
            $this->assertFalse(file_exists($goodPath.$i));
            $ids[$i] = \Yii::app()->yiiq->enqueueJob('\Yiiq\test\jobs\GoodJob', ['file' => $goodFile.$i], $queue);
            $this->assertFalse(\Yii::app()->yiiq->isCompleted($ids[$i]));
            $this->assertFalse(\Yii::app()->yiiq->isFailed($ids[$i]));
        }

        $this->startYiiq($queue, $threads);

        $this->waitForJobs($threads, 20);
        usleep(self::TIME_TO_START);

        $this->assertEquals(0, filesize($logPath));
        for ($i = 1; $i < 20; $i++) {
            $this->assertTrue(file_exists($goodPath.$i));
            $this->assertTrue(\Yii::app()->yiiq->isCompleted($ids[$i]));
            $this->assertFalse(\Yii::app()->yiiq->isFailed($ids[$i]));
            $this->assertFalse(\Yii::app()->yiiq->isExecuting($ids[$i]));
        }

        $this->assertTrue(\Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }
}
