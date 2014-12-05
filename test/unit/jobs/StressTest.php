<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains stress tests for jobs.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.unit.jobs
 */

namespace Yiiq\test\unit\jobs;

use Yiiq\test\cases\Job;

class StressTest extends Job
{
    public function testManyJobs()
    {
        $procTitle  = $this->getBaseProcessTitle();
        $logPath    = $this->getLogPath();

        $queue      = 'default_'.TEST_TOKEN;
        $threads    = 20;

        $this->assertNotContains($procTitle, $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $results = [];
        for ($i = 0; $i < 200; $i++) {
            $result = rand();
            $job = \Yii::app()->yiiq->enqueue('\Yiiq\test\jobs\ReturnJob', ['result' => $result], $queue);

            $results[$job->id] = $result;
        }

        $this->waitForJobs($threads, 100);

        $this->stopYiiq();

        $this->assertNotContains($procTitle, $this->exec('ps aux'));
        $size = filesize($logPath);
        $this->assertEquals(0, $size);

        $this->startYiiq($queue, $threads);
        usleep(self::TIME_TO_START);
        $this->assertContains($procTitle, $this->exec('ps aux'));

        for ($i = 0; $i < 200; $i++) {
            $result = rand();
            $job = \Yii::app()->yiiq->enqueueAfter(0, '\Yiiq\test\jobs\ReturnJob', ['result' => $result], $queue);

            $results[$job->id] = $result;
        }

        $this->waitForJobs($threads, 100);

        $this->stopYiiq();

        $this->assertNotContains($procTitle, $this->exec('ps aux'));
        $size = filesize($logPath);
        $this->assertEquals(0, $size);

        $this->startYiiq($queue, $threads);
        usleep(self::TIME_TO_START);
        $this->assertContains($procTitle, $this->exec('ps aux'));

        $this->waitForJobs($threads, 200);

        $size = filesize($logPath);
        $this->assertEquals(0, $size);

        echo file_get_contents($logPath);
        foreach ($results as $id => $result) {
            $job = \Yii::app()->yiiq->get($id);
            $this->assertTrue($job->isCompleted());
            $this->assertFalse($job->isFailed());
            $this->assertFalse($job->isExecuting());
            $this->assertEquals($result, $job->getResult());
        }

        $this->assertContains($procTitle, $this->exec('ps aux'));
        $this->assertTrue(\Yii::app()->yiiq->health->check(false));
        $this->stopYiiq();
    }
}
