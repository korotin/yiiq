<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains tests for Yiiq command.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.unit.common
 */

namespace Yiiq\test\unit\common;

use Yiiq\test\cases\Base;

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

    public function testStatus()
    {
        $this->assertEquals(
            $this->execYiic('yiiq status'),
            'No processes found. System is not working.'
        );

        $this->assertNotContains($this->getBaseProcessTitle(), $this->exec('ps aux'));
        $this->startYiiq();

        usleep(self::TIME_TO_START);

        $this->assertContains($this->getBaseProcessTitle(), $this->exec('ps aux'));

        $output = $this->execYiic('yiiq status');
        $matches = null;
        $this->assertEquals(
            1,
            preg_match('/All processes \((\d+)\) are alive\. Everything looks good\./', $output, $matches)
        );

        posix_kill((int) $matches[1], SIGKILL);

        $this->assertNotContains($this->getBaseProcessTitle(), $this->exec('ps aux'));

        $this->assertEquals(
            $this->execYiic('yiiq status'),
            'All processes ('.$matches[1].') are dead. System is not working.'
        );

        $this->stopYiiq();

        $this->assertEquals(
            $this->execYiic('yiiq status'),
            'No processes found. System is not working.'
        );

        $this->assertNotContains($this->getBaseProcessTitle(), $this->exec('ps aux'));
        $this->startYiiq();

        usleep(self::TIME_TO_START);

        $this->assertContains($this->getBaseProcessTitle(), $this->exec('ps aux'));

        $output = $this->execYiic('yiiq status');
        $this->assertRegExp(
            '/All processes \(\d+\) are alive\. Everything looks good\./',
            $output
        );

        $this->stopYiiq();

        $this->assertEquals(
            $this->execYiic('yiiq status'),
            'No processes found. System is not working.'
        );
    }
}
