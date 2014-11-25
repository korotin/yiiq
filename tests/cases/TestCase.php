<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains base test case.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.cases
 */

namespace Yiiq\tests\cases;

abstract class TestCase extends \CTestCase
{
    protected $started = false;

    protected function exec($cmd)
    {
        $lines = array();
        exec($cmd, $lines);
        return implode("\n", $lines);
    }

    protected function createCommand()
    {
        $command = new \Yiiq\commands\Main(null, null);
        return $command;
    }

    protected function startYiiq($queue = 'default', $threads = 5)
    {
        if ($this->started)
            throw new \CException('Yiiq already started');

        $this->started = true;

        $command = $this->createCommand();
        ob_start();
        if (!is_array($queue)) $queue = [$queue];
        $command->actionStart($queue, $threads, 'yiiq.log');
        ob_end_clean();
    }

    protected function stopYiiq()
    {
        if (!$this->started)
            throw new CException('Yiiq is not started');

        $this->started = false;

        $command = $this->createCommand();
        ob_start();
        $command->actionStop();
        ob_end_clean();
    }

    protected function cleanup()
    {
        if ($this->started) {
            $this->stopYiiq();
        }

        $this->exec('rm -rf '.__DIR__.'/../runtime/*');

        $keys = \Yii::app()->redis->keys('*');
        foreach ($keys as $key) {
            \Yii::app()->redis->del(preg_replace('/^'.\Yii::app()->redis->prefix.'/', '', $key));
        }
    }

    public function setUp()
    {
        $this->cleanup();
    }

    public function tearDown()
    {
        $this->cleanup();
    }

}