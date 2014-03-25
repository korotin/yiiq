<?php

class Yiiq extends CApplicationComponent {

    const DEFAULT_QUEUE = 'default';

    const TYPE_SCHEDULED = 'scheduled';
    
    public $prefix = 'yiiq';

    protected $pidPool;
    protected $queues = array();

    protected function serializeJob($class, array $args= array(), $name = null)
    {
        return CJSON::encode(compact('class', 'args', 'name'));
    }

    protected function deserealizeJob($job)
    {
        return CJSON::decode($job);
    }

    protected function getJobKey($id)
    {
        return $this->prefix.':job:'.$id;
    }

    public function getPidPool()
    {
        if ($this->pidPool === null) {
            $this->pidPool = new ARedisSet($this->prefix.':pids');
        }

        return $this->pidPool;
    }

    public function getQueue($queue = self::DEFAULT_QUEUE, $type = self::TYPE_SCHEDULED)
    {
        if (!$queue) {
            $queue = self::DEFAULT_QUEUE;
        }

        if (!isset($this->queues[$queue])) {
            $this->queues[$queue] = new ARedisSet($this->prefix.':queue:'.$queue.':'.$type);
        }

        return $this->queues[$queue];
    }

    public function enqueueJob($class, array $args = array(), $name = '', $queue = self::DEFAULT_QUEUE)
    {
        $id = md5(microtime(true));
        $key = $this->getJobKey($id);
        
        Yii::app()->redis->getClient()->set(
            $key,
            $this->serializeJob($class, $args, $name)
        );
        $this->getQueue($queue)->add($id);

        return $id;
    }

    public function popJobId($queue = self::DEFAULT_QUEUE)
    {
        $id = $this->getQueue($queue)->pop();
       
        return $id;
    }

    public function deleteJob($id)
    {
        $key = $this->getJobKey($id);
        Yii::app()->redis->getClient()->del($key);
    }

    public function getJob($id)
    {
        $key = $this->getJobKey($id);
        $job = Yii::app()->redis->getClient()->get($key);
        if (!$job) return;

        return $this->deserealizeJob($job); 
    }

}