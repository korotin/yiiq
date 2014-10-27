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
     * Job type.
     * 
     * @var string
     */
    protected $type;

    /**
     * Job id.
     * 
     * @var string
     */
    protected $id;

    /**
     * Arguments.
     * 
     * @var array
     */
    protected $args;

    public function __construct($queue, $type, $id)
    {
        $this->queue = $queue;
        $this->type = $type;
        $this->id = $id;
    }

    /**
     * Abstract method in which all the job will be done.
     */
    abstract protected function run();

    /**
     * This method is invoked by worker.
     * It sets job arguments and runs job itself.
     * 
     * @param  array $args
     * @return mixed job result returned by run()
     */
    public function execute($args)
    {
        if ($args && is_array($args)) {
            foreach ($args as $k => $v) {
                $this->$k = $v;
            }

            $this->args = $args;
        }

        return $this->run();
    }

}