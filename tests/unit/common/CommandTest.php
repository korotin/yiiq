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

use Yiiq\tests\cases\Base;

class CommandTest extends Base
{
    public function testStartAndStop()
    {
        $this->assertNotContains($this->getBaseProcessTitle(), $this->exec('ps aux'));
        $this->startYiiq();
        usleep(self::TIME_TO_START);
        $this->assertContains($this->getBaseProcessTitle(), $this->exec('ps aux'));
        $this->stopYiiq();
        $this->assertNotContains($this->getBaseProcessTitle(), $this->exec('ps aux'));

        $this->assertFileExists($this->getLogPath());
        $this->assertEquals(0, filesize($this->getLogPath()));
    }
}
