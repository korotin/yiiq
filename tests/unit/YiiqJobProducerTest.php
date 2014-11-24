<?php

class YiiqJobProducerTest extends YiiqBaseTestCase
{
    public function testDefaults()
    {
        $producer = Yii::app()->yiiq->createJob('YiiqGoodJob');
        $this->assertInstanceOf('YiiqJobProducer', $producer);
        $this->assertEquals('YiiqGoodJob', $producer->class);
        $this->assertEquals(Yiiq::TYPE_SIMPLE, $producer->type);
        $this->assertEquals(Yiiq::DEFAULT_QUEUE, $producer->queue);
    }
}