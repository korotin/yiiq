<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains tests for good scheduled jobs.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.unit.jobs.scheduled
 */

namespace Yiiq\tests\unit\jobs;

use Yiiq\tests\cases\Job;

class GoodTest extends Job
{
    /**
     * @dataProvider startParametersProvider
     */
    public function testScheduledJob($queue, $threads)
    {
        $procTitle  = $this->getBaseProcessTitle();
        $logPath    = $this->getLogPath();
        $goodAtFile = 'goodjob_at_'.TEST_TOKEN;
        $goodAtPath = $this->getRuntimePath().'/'.$goodAtFile;
        $goodInFile = 'goodjob_in_'.TEST_TOKEN;
        $goodInPath = $this->getRuntimePath().'/'.$goodInFile;

        $this->assertNotContains($procTitle, $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $this->assertFalse(file_exists($goodAtPath));
        $this->assertFalse(file_exists($goodInPath));
        $ids = [];
        $ids[] = \Yii::app()->yiiq->enqueueJobAt(time() + 2, '\Yiiq\tests\jobs\GoodJob', ['file' => $goodAtFile], $queue);
        $ids[] = \Yii::app()->yiiq->enqueueJobIn(2, '\Yiiq\tests\jobs\GoodJob', ['file' => $goodInFile], $queue);

        usleep(self::TIME_FOR_JOB);

        foreach ($ids as $id) {
            $this->assertTrue(\Yii::app()->yiiq->hasJob($id));
            $this->assertFalse(\Yii::app()->yiiq->isExecuting($id));
            $this->assertFalse(\Yii::app()->yiiq->isCompleted($id));
            $this->assertFalse(\Yii::app()->yiiq->isFailed($id));
        }

        usleep(self::TIME_FOR_JOB + 2000000);
        $this->waitForJobs($threads, 2);

        $this->assertTrue(file_exists($goodAtPath));
        $this->assertTrue(file_exists($goodInPath));
        foreach ($ids as $id) {
            $this->assertFalse(\Yii::app()->yiiq->hasJob($id));
            $this->assertTrue(\Yii::app()->yiiq->isCompleted($id));
            $this->assertFalse(\Yii::app()->yiiq->isExecuting($id));
            $this->assertFalse(\Yii::app()->yiiq->isFailed($id));
        }

        $this->assertTrue(\Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }
}