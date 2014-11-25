<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains stress tests for jobs.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.unit.jobs
 */

namespace Yiiq\tests\unit\jobs;

use Yiiq\tests\cases\JobCase;

class StressJobTest extends JobCase
{
    public function testManyJobs()
    {
        $queue = 'default';
        $threads = 20;

        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $results = [];
        for ($i = 0; $i < 200; $i++) {
            $result = rand();
            $id = \Yii::app()->yiiq->enqueueJob('\Yiiq\tests\jobs\ReturnJob', ['result' => $result], $queue);

            $results[$id] = $result; 
        }

        $this->waitForJobs($threads, 100);

        $this->stopYiiq();

        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $size = filesize(__DIR__.'/../../runtime/yiiq.log');
        $this->assertEquals(0, $size);

        $this->startYiiq($queue, $threads);
        usleep(self::TIME_TO_START);
        $this->assertContains('YiiqTest', $this->exec('ps aux'));

        for ($i = 0; $i < 200; $i++) {
            $result = rand();
            $id = \Yii::app()->yiiq->enqueueJobIn(0, '\Yiiq\tests\jobs\ReturnJob', ['result' => $result], $queue);

            $results[$id] = $result; 
        }

        $this->waitForJobs($threads, 100);

        $this->stopYiiq();

        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $size = filesize(__DIR__.'/../../runtime/yiiq.log');
        $this->assertEquals(0, $size);

        $this->startYiiq($queue, $threads);
        usleep(self::TIME_TO_START);
        $this->assertContains('YiiqTest', $this->exec('ps aux'));

        $this->waitForJobs($threads, 200);

        $size = filesize(__DIR__.'/../../runtime/yiiq.log');
        $this->assertEquals(0, $size);

        foreach ($results as $id => $result) {
            $this->assertTrue(\Yii::app()->yiiq->isCompleted($id));
            $this->assertFalse(\Yii::app()->yiiq->isFailed($id));
            $this->assertFalse(\Yii::app()->yiiq->isExecuting($id));
            $this->assertEquals($result, \Yii::app()->yiiq->getJobResult($id));
        }

        $this->assertTrue(\Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }
}
