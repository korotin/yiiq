<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains base test case.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.cases
 */

namespace yiiq\tests\cases;

abstract class Base extends \CTestCase
{
    const TIME_TO_START = 500000;

    protected $started = false;

    protected function getBaseProcessTitle()
    {
        return '['.\Yii::app()->yiiq->name.']';
    }

    protected function getIdleWorkerTitle($queue, $threads)
    {
        return '[YiiqTest] worker@'.$queue.': no new jobs (0 of '.$threads.' threads busy)';
    }

    protected function getRuntimePath()
    {
        return __DIR__.'/../runtime';
    }

    protected function getLogName()
    {
        return 'yiiq_'.TEST_TOKEN.'.log';
    }

    protected function getLogPath()
    {
        return $this->getRuntimePath().'/'.$this->getLogName();
    }

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

    protected function startYiiq($queue = null, $threads = 5)
    {
        if ($this->started) {
            throw new \CException('Yiiq already started');
        }

        $this->started = true;

        $command = $this->createCommand();
        ob_start();
        if (!$queue) {
            $queue = 'default_'.TEST_TOKEN;
        }
        if (!is_array($queue)) {
            $queue = [$queue];
        }
        $command->actionStart($queue, $threads, $this->getLogName());
        ob_end_clean();
    }

    protected function stopYiiq()
    {
        if (!$this->started) {
            throw new CException('Yiiq is not started');
        }

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

        $this->exec('rm -rf '.__DIR__.'/../runtime/*'.TEST_TOKEN.'*');

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
