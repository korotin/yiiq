<?php

class YiiqTest extends YiiqBaseTestCase
{
    
    public function testSimpleJob()
    {
        $this->startYiiq();

        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'));
        Yii::app()->yiiq->enqueueJob('YiiqGoodJob');
        sleep(2);
        $this->assertEquals(0, filesize(__DIR__.'/../runtime/yiiq.log'));
        $this->assertTrue(file_exists(__DIR__.'/../runtime/goodjob'));

        $this->stopYiiq();
    }

    public function testManySimpleJobs()
    {
        $this->startYiiq();

        for ($i = 1; $i < 10; $i++) {
            $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'.$i));
            Yii::app()->yiiq->enqueueJob('YiiqGoodJob', array('file' => 'goodjob'.$i));
        }
        sleep(3);
        $this->assertEquals(0, filesize(__DIR__.'/../runtime/yiiq.log'));
        for ($i = 1; $i < 10; $i++) {
            $this->assertTrue(file_exists(__DIR__.'/../runtime/goodjob'.$i));
        }

        $this->stopYiiq();
    }

    public function testBadJob()
    {
        $this->startYiiq();

        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'));
        Yii::app()->yiiq->enqueueJob('YiiqBadJob');
        sleep(2);
        $size = filesize(__DIR__.'/../runtime/yiiq.log');
        $this->assertGreaterThan(0, $size);
        $this->assertContains('YiiqTest', $this->exec('ps aux'));

        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'));
        Yii::app()->yiiq->enqueueJob('YiiqGoodJob');
        sleep(2);
        $this->assertEquals($size, filesize(__DIR__.'/../runtime/yiiq.log'));
        $this->assertTrue(file_exists(__DIR__.'/../runtime/goodjob'));

        $this->stopYiiq();
    }

}