<?php

class YiiqCommandTest extends YiiqBaseTestCase
{
    
    public function testStartAndStop()
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq();
        usleep(100000);
        $this->assertContains('YiiqTest', $this->exec('ps aux'));
        $this->stopYiiq();
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));

        $this->assertFileExists(__DIR__.'/../runtime/yiiq.log');
        $this->assertEquals(0, filesize(__DIR__.'/../runtime/yiiq.log'));
    }

}