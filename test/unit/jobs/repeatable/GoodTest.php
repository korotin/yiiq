<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains tests for good repeatable jobs.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.unit.jobs.repeatable
 */

namespace Yiiq\test\unit\jobs\repeatable;

use Yiiq\test\cases\Job;

class GoodTest extends Job
{
    /**
     * @dataProvider startParametersProvider
     */
    public function testRepeatableJob($queue, $threads)
    {
        $procTitle  = $this->getBaseProcessTitle();
        $goodFile   = 'goodjob_'.TEST_TOKEN;
        $goodPath   = $this->getRuntimePath().'/'.$goodFile;
        $logPath    = $this->getLogPath();

        $this->assertNotContains($procTitle, $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $job = \Yii::app()->yiiq->enqueueRepeatable(
            'goodjob',
            1,
            '\Yiiq\test\jobs\GoodJob',
            ['content' => '*', 'file' => $goodFile],
            $queue
        );

        usleep(5000000);

        $size = filesize($logPath);
        $this->assertEquals(0, $size);
        $contentSize = filesize($goodPath);
        $this->assertGreaterThanOrEqual(4, $contentSize);
        $this->assertLessThanOrEqual(6, $contentSize);

        $this->assertTrue(\Yii::app()->yiiq->exists('goodjob'));
        $this->assertFalse($job->isCompleted());
        $this->assertFalse($job->isFailed());

        \Yii::app()->yiiq->delete('goodjob');
        $this->assertFalse(\Yii::app()->yiiq->exists('goodjob'));

        usleep(200000);

        unlink($goodPath);

        usleep(600000);

        $this->assertFalse(file_exists($goodPath));

        $this->assertTrue(\Yii::app()->yiiq->health->check(false));
        $this->stopYiiq();
    }
}
