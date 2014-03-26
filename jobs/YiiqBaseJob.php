<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains Yiiq base job class.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package ext.yiiq.jobs
 */

/**
 * Yiiq base job class.
 * All jobs must inherit this class.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
abstract class YiiqBaseJob
{
    /**
     * Queue name.
     * 
     * @var string
     */
    protected $queue;

    /**
     * Job id.
     * 
     * @var string
     */
    protected $jobId;

    public function __construct($queue, $jobId)
    {
        $this->queue = $queue;
        $this->jobId = $jobId;
    }

    /**
     * Abstract method in which all the job will be done.
     */
    abstract protected function run();

    /**
     * This method is invoked by worker.
     * It set job arguments and runs job itself.
     * 
     * @param  array $args
     */
    public function execute($args)
    {
        foreach ($args as $k => $v) {
            $this->$k = $v;
        }

        $this->run();
    }

}