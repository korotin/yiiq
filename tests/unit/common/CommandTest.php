<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains tests for Yiiq command.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.unit.common
 */

namespace Yiiq\tests\unit\common;

use Yiiq\tests\cases\TestCase;

class CommandTest extends TestCase
{
    
    public function testStartAndStop()
    {
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));
        $this->startYiiq();
        usleep(100000);
        $this->assertContains('YiiqTest', $this->exec('ps aux'));
        $this->stopYiiq();
        $this->assertNotContains('YiiqTest', $this->exec('ps aux'));

        $this->assertFileExists(__DIR__.'/../../runtime/yiiq.log');
        $this->assertEquals(0, filesize(__DIR__.'/../../runtime/yiiq.log'));
    }

}