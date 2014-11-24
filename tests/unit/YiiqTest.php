<?php

class YiiqTest extends YiiqBaseTestCase
{
    public function startParametersProvider()
    {
        return [
            ['default', 1],
            ['default', 5],
            ['default', 10],
            ['default', 15],
            ['custom', 1],
            ['custom', 5],
            ['custom', 10],
            ['custom', 15],
        ];
    }

    public function badClassProvider()
    {
        return [
            ['YiiqBadJob'], 
            ['YiiqBadJob2'], 
            ['YiiqBadJob3'],
        ];
    }

    public function startParametersAndBadClassProvider()
    {
        $parameters = $this->startParametersProvider();
        $badClasses = $this->badClassProvider();

        $data = [];
        foreach ($parameters as $parameter){
            foreach ($badClasses as $badClass) {
                $data[] = array_merge($parameter, $badClass);
            }
        }

        return $data;
    }
    
    /**
     * @dataProvider startParametersProvider
     */
    public function testSimpleJob($queue, $threads)
    {
        $this->startYiiq($queue, $threads);

        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'));
        $id = Yii::app()->yiiq->enqueueJob('YiiqGoodJob', [], $queue);
        $this->waitForJobs($threads, 1);
        $this->assertEquals(0, filesize(__DIR__.'/../runtime/yiiq.log'));
        $this->assertTrue(file_exists(__DIR__.'/../runtime/goodjob'));
        $this->assertTrue(Yii::app()->yiiq->isCompleted($id));
        $this->assertFalse(Yii::app()->yiiq->isFailed($id));
        $this->assertFalse(Yii::app()->yiiq->isExecuting($id));

        $this->assertTrue(Yii::app()->yiiq->check(false));
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
            $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'.$i));
            $ids[$i] = Yii::app()->yiiq->enqueueJob('YiiqGoodJob', ['file' => 'goodjob'.$i], $queue);
            $this->assertFalse(Yii::app()->yiiq->isCompleted($ids[$i]));
            $this->assertFalse(Yii::app()->yiiq->isFailed($ids[$i]));
        }

        $this->waitForJobs($threads, 20);

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

    /**
     * @dataProvider startParametersProvider
     */
    public function testManySimpleJobsBeforeStart($queue, $threads)
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));

        $ids = [];
        for ($i = 1; $i < 20; $i++) {
            $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'.$i));
            $ids[$i] = Yii::app()->yiiq->enqueueJob('YiiqGoodJob', ['file' => 'goodjob'.$i], $queue);
            $this->assertFalse(Yii::app()->yiiq->isCompleted($ids[$i]));
            $this->assertFalse(Yii::app()->yiiq->isFailed($ids[$i]));
        }

        $this->startYiiq($queue, $threads);

        $this->waitForJobs($threads, 20);
        
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

    /**
     * @dataProvider startParametersAndBadClassProvider
     */
    public function testBadSimpleJob($queue, $threads, $badClass)
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'));
        $id = Yii::app()->yiiq->enqueueJob($badClass, [], $queue);
        $this->waitForJobs($threads, 1, true);
        
        $size = filesize(__DIR__.'/../runtime/yiiq.log');
        $this->assertGreaterThan(0, $size);
        $this->assertContains('YiiqTest', $this->exec('ps aux'));

        $this->assertFalse(Yii::app()->yiiq->isCompleted($id));
        $this->assertFalse(Yii::app()->yiiq->isExecuting($id));
        $this->assertTrue(Yii::app()->yiiq->isFailed($id));

        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'));
        Yii::app()->yiiq->enqueueJob('YiiqGoodJob', [], $queue);
        $this->waitForJobs($threads, 1);
        $this->assertEquals($size, filesize(__DIR__.'/../runtime/yiiq.log'));
        $this->assertTrue(file_exists(__DIR__.'/../runtime/goodjob'));

        $this->assertTrue(Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    /**
     * @dataProvider startParametersAndBadClassProvider
     */
    public function testManyBadSimpleJobs($queue, $threads, $badClass)
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'));
        $ids = [];
        for ($i = 0; $i < 10; $i++) {
            $ids[] = Yii::app()->yiiq->enqueueJob($badClass, [], $queue);
        }
        $this->waitForJobs($threads, 10, true);
        
        $size = filesize(__DIR__.'/../runtime/yiiq.log');
        $this->assertGreaterThan(0, $size);
        $this->assertContains('YiiqTest', $this->exec('ps aux'));

        foreach ($ids as $id) {
            $this->assertFalse(Yii::app()->yiiq->isCompleted($id));
            $this->assertFalse(Yii::app()->yiiq->isExecuting($id));
            $this->assertTrue(Yii::app()->yiiq->isFailed($id));
        }

        $this->assertContains('YiiqTest', $this->exec('ps aux'));
        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'));
        Yii::app()->yiiq->enqueueJob('YiiqGoodJob', [], $queue);
        $this->waitForJobs($threads, 1);
        $this->assertEquals($size, filesize(__DIR__.'/../runtime/yiiq.log'));
        $this->assertContains('YiiqTest', $this->exec('ps aux'));
        $this->assertTrue(file_exists(__DIR__.'/../runtime/goodjob'));

        $this->assertTrue(Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    /**
     * @dataProvider startParametersProvider
     */
    public function testScheduledJob($queue, $threads)
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob_at'));
        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob_in'));
        $ids = [];
        $ids[] = Yii::app()->yiiq->enqueueJobAt(time() + 2, 'YiiqGoodJob', array('file' => 'goodjob_at'), $queue);
        $ids[] = Yii::app()->yiiq->enqueueJobIn(2, 'YiiqGoodJob', array('file' => 'goodjob_in'), $queue);

        usleep(500000);

        foreach ($ids as $id) {
            $this->assertTrue(Yii::app()->yiiq->hasJob($id));
            $this->assertFalse(Yii::app()->yiiq->isExecuting($id));
            $this->assertFalse(Yii::app()->yiiq->isCompleted($id));
            $this->assertFalse(Yii::app()->yiiq->isFailed($id));
        }

        usleep(1500000);
        $this->waitForJobs($threads, 2);

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

    /**
     * @dataProvider startParametersAndBadClassProvider
     */
    public function testBadScheduledJob($queue, $threads, $badClass)
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'));
        Yii::app()->yiiq->enqueueJobIn(2, $badClass, [], $queue);
        
        usleep(3000000);
        $this->waitForJobs($threads, 1, true);

        $size = filesize(__DIR__.'/../runtime/yiiq.log');
        $this->assertGreaterThan(0, $size);
        $this->assertContains('YiiqTest', $this->exec('ps aux'));

        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'));
        Yii::app()->yiiq->enqueueJobIn(2, 'YiiqGoodJob', [], $queue);
        
        usleep(3000000);
        $this->waitForJobs($threads, 1);

        $this->assertEquals($size, filesize(__DIR__.'/../runtime/yiiq.log'));
        $this->assertTrue(file_exists(__DIR__.'/../runtime/goodjob'));

        $this->assertTrue(Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    /**
     * @dataProvider startParametersProvider
     */
    public function testRepeatableJob($queue, $threads)
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        Yii::app()->yiiq->enqueueRepeatableJob('goodjob', 1, 'YiiqGoodJob', ['content' => '*'], $queue);

        usleep(5000000);

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

        usleep(600000);
        
        $this->assertFalse(file_exists(__DIR__.'/../runtime/goodjob'));

        $this->assertTrue(Yii::app()->yiiq->check(false));
        $this->stopYiiq();
    }

    /**
     * @dataProvider startParametersAndBadClassProvider
     */
    public function testBadRepeatableJob($queue, $threads, $badClass)
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        Yii::app()->yiiq->enqueueRepeatableJob('badjob', 1, $badClass, [], $queue);

        $this->waitForJobs($threads, 1, true);

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

    /**
     * @dataProvider startParametersProvider
     */
    public function testJobStates($queue, $threads)
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $id = Yii::app()->yiiq->enqueueJob('YiiqWaitJob', ['sleep' => 2], $queue);

        usleep(200000);

        $this->assertTrue(Yii::app()->yiiq->isExecuting($id));

        usleep(2200000);

        $this->assertFalse(Yii::app()->yiiq->isFailed($id));
        $this->assertTrue(Yii::app()->yiiq->isCompleted($id));
        $this->assertFalse(Yii::app()->yiiq->isExecuting($id));

        $this->stopYiiq();
    }

    /**
     * @dataProvider startParametersProvider
     */
    public function testResultSaving($queue, $threads)
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq($queue, $threads);

        $result = rand();
        $id = Yii::app()->yiiq->enqueueJob('YiiqReturnJob', ['result' => $result], $queue);
        
        $this->waitForJobs($threads, 1);

        $size = filesize(__DIR__.'/../runtime/yiiq.log');
        $this->assertEquals(0, $size);
        $this->assertFalse(Yii::app()->yiiq->isFailed($id));
        $this->assertTrue(Yii::app()->yiiq->isCompleted($id));
        $this->assertFalse(Yii::app()->yiiq->isExecuting($id));
        $this->assertEquals($result, Yii::app()->yiiq->getJobResult($id));
        $this->assertEquals($result, Yii::app()->yiiq->getJobResult($id, true));
        $this->assertNull(Yii::app()->yiiq->getJobResult($id));

        $this->stopYiiq();
    }

}