<?php

class YiiqTest extends YiiqBaseTestCase
{
    
    public function testSimpleJob()
    {
        $this->startYiiq();

        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'));
        $id = Yii::app()->yiiq->enqueueJob('YiiqGoodJob');
        usleep(300000);
        $this->assertEquals(0, filesize(__DIR__.'/../runtime/yiiq.log'));
        $this->assertTrue(file_exists(__DIR__.'/../runtime/goodjob'));
        $this->assertTrue(Yii::app()->yiiq->isCompleted($id));
        $this->assertFalse(Yii::app()->yiiq->isFailed($id));
        $this->assertFalse(Yii::app()->yiiq->isExecuting($id));

        $this->assertTrue(Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    public function testManySimpleJobsAfterStart()
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq();

        $ids = array();
        for ($i = 1; $i < 20; $i++) {
            $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'.$i));
            $ids[$i] = Yii::app()->yiiq->enqueueJob('YiiqGoodJob', array('file' => 'goodjob'.$i));
            $this->assertFalse(Yii::app()->yiiq->isCompleted($ids[$i]));
            $this->assertFalse(Yii::app()->yiiq->isFailed($ids[$i]));
        }

        sleep(2);

        $this->assertEquals(0, filesize(__DIR__.'/../runtime/yiiq.log'));
        for ($i = 1; $i < 20; $i++) {
            $this->assertTrue(file_exists(__DIR__.'/../runtime/goodjob'.$i));
            $this->assertTrue(Yii::app()->yiiq->isCompleted($ids[$i]));
            $this->assertFalse(Yii::app()->yiiq->isFailed($ids[$i]));
            $this->assertFalse(Yii::app()->yiiq->isExecuting($ids[$i]));
        }

        $this->assertTrue(Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    public function testManySimpleJobsBeforeStart()
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));

        $ids = array();
        for ($i = 1; $i < 20; $i++) {
            $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'.$i));
            $ids[$i] = Yii::app()->yiiq->enqueueJob('YiiqGoodJob', array('file' => 'goodjob'.$i));
            $this->assertFalse(Yii::app()->yiiq->isCompleted($ids[$i]));
            $this->assertFalse(Yii::app()->yiiq->isFailed($ids[$i]));
        }

        $this->startYiiq();

        sleep(2);
        
        $this->assertEquals(0, filesize(__DIR__.'/../runtime/yiiq.log'));
        for ($i = 1; $i < 20; $i++) {
            $this->assertTrue(file_exists(__DIR__.'/../runtime/goodjob'.$i));
            $this->assertTrue(Yii::app()->yiiq->isCompleted($ids[$i]));
            $this->assertFalse(Yii::app()->yiiq->isFailed($ids[$i]));
            $this->assertFalse(Yii::app()->yiiq->isExecuting($ids[$i]));
        }

        $this->assertTrue(Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    public function testBadSimpleJob()
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq();

        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'));
        $id = Yii::app()->yiiq->enqueueJob('YiiqBadJob');
        usleep(2000000);
        $size = filesize(__DIR__.'/../runtime/yiiq.log');
        $this->assertGreaterThan(0, $size);
        $this->assertContains('YiiqTest', $this->exec('ps aux'));

        $this->assertFalse(Yii::app()->yiiq->isCompleted($id));
        $this->assertFalse(Yii::app()->yiiq->isExecuting($id));
        $this->assertTrue(Yii::app()->yiiq->isFailed($id));

        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'));
        Yii::app()->yiiq->enqueueJob('YiiqGoodJob');
        usleep(300000);
        $this->assertEquals($size, filesize(__DIR__.'/../runtime/yiiq.log'));
        $this->assertTrue(file_exists(__DIR__.'/../runtime/goodjob'));

        $this->assertTrue(Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    public function testScheduledJob()
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq();

        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob_at'));
        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob_in'));
        $ids = array();
        $ids[] = Yii::app()->yiiq->enqueueJobAt(time() + 2, 'YiiqGoodJob', array('file' => 'goodjob_at'));
        $ids[] = Yii::app()->yiiq->enqueueJobIn(2, 'YiiqGoodJob', array('file' => 'goodjob_in'));

        usleep(600000);

        foreach ($ids as $id) {
            $this->assertTrue(Yii::app()->yiiq->hasJob($id));
            $this->assertFalse(Yii::app()->yiiq->isExecuting($id));
            $this->assertFalse(Yii::app()->yiiq->isCompleted($id));
            $this->assertFalse(Yii::app()->yiiq->isFailed($id));
        }

        usleep(2200000);

        $this->assertTrue(file_exists(__DIR__.'/../runtime/goodjob_at'));
        $this->assertTrue(file_exists(__DIR__.'/../runtime/goodjob_in'));
        foreach ($ids as $id) {
            $this->assertFalse(Yii::app()->yiiq->hasJob($id));
            $this->assertTrue(Yii::app()->yiiq->isCompleted($id));
            $this->assertFalse(Yii::app()->yiiq->isExecuting($id));
            $this->assertFalse(Yii::app()->yiiq->isFailed($id));
        }

        $this->assertTrue(Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    public function testBadScheduledJob()
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq();

        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'));
        Yii::app()->yiiq->enqueueJobIn(2, 'YiiqBadJob');
        usleep(2200000);
        $size = filesize(__DIR__.'/../runtime/yiiq.log');
        $this->assertGreaterThan(0, $size);
        $this->assertContains('YiiqTest', $this->exec('ps aux'));

        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'));
        Yii::app()->yiiq->enqueueJobIn(2, 'YiiqGoodJob');
        usleep(2200000);
        $this->assertEquals($size, filesize(__DIR__.'/../runtime/yiiq.log'));
        $this->assertTrue(file_exists(__DIR__.'/../runtime/goodjob'));

        $this->assertTrue(Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    public function testRepeatableJob()
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq();

        Yii::app()->yiiq->enqueueRepeatableJob('goodjob', 1, 'YiiqGoodJob', ['content' => '*']);

        sleep(5);

        $size = filesize(__DIR__.'/../runtime/yiiq.log');
        $this->assertEquals(0, $size);
        $contentSize = filesize(__DIR__.'/../runtime/goodjob');
        $this->assertGreaterThanOrEqual(4, $contentSize);
        $this->assertLessThanOrEqual(6, $contentSize);

        $this->assertTrue(Yii::app()->yiiq->hasJob('goodjob'));
        $this->assertFalse(Yii::app()->yiiq->isCompleted('goodjob'));
        $this->assertFalse(Yii::app()->yiiq->isFailed('goodjob'));

        Yii::app()->yiiq->deleteJob('goodjob');
        $this->assertFalse(Yii::app()->yiiq->hasJob('goodjob'));
        usleep(500000);
        unlink(__DIR__.'/../runtime/goodjob');
        usleep(700000);
        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'));

        $this->assertTrue(Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    public function testBadRepeatableJob()
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq();

        Yii::app()->yiiq->enqueueRepeatableJob('badjob', 1, 'YiiqBadJob');

        sleep(Yii::app()->yiiq->maxFaults + 1);
        $size = filesize(__DIR__.'/../runtime/yiiq.log');
        $this->assertGreaterThan(0, $size);
        $this->assertContains('YiiqTest', $this->exec('ps aux'));

        $this->assertTrue(Yii::app()->yiiq->hasJob('badjob'));
        $this->assertFalse(Yii::app()->yiiq->isCompleted('badjob'));
        $this->assertFalse(Yii::app()->yiiq->isExecuting('badjob'));
        $this->assertTrue(Yii::app()->yiiq->isFailed('badjob'));

        $this->assertTrue(Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    public function testJobStates()
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq();

        $id = Yii::app()->yiiq->enqueueJob('YiiqDummyJob', ['sleep' => 2]);
        usleep(200000);
        $this->assertTrue(Yii::app()->yiiq->isExecuting($id));
        usleep(2200000);
        $this->assertFalse(Yii::app()->yiiq->isFailed($id));
        $this->assertTrue(Yii::app()->yiiq->isCompleted($id));
        $this->assertFalse(Yii::app()->yiiq->isExecuting($id));

        $this->stopYiiq();
    }

}