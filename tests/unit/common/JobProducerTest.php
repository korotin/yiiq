<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains tests for job producer.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.unit.common
 */

namespace Yiiq\tests\unit\common;

use Yiiq\tests\cases\Base;

class JobProducerTest extends Base
{
    public function testDefaults()
    {
        $producer = \Yii::app()->yiiq->createJob('\Yiiq\tests\helpers\GoodJob');
        $this->assertInstanceOf('\Yiiq\jobs\Producer', $producer);
        $this->assertEquals('\Yiiq\tests\helpers\GoodJob', $producer->class);
        $this->assertEquals(\Yiiq\Yiiq::TYPE_SIMPLE, $producer->type);
        $this->assertEquals(\Yiiq\Yiiq::DEFAULT_QUEUE, $producer->queue);
    }
}