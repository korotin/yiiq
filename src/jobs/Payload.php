<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains payload class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.jobs
 */

namespace Yiiq\jobs;

/**
 * Payload class.
 * All jobs must inherit this class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
abstract class Payload
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

    public function __construct(Job $job)
    {
        $metadata = $job->metadata;

        $this->queue    = $metadata->queue;
        $this->type     = $metadata->type;
        $this->id       = $metadata->id;
    }

    /**
     * Abstract method in which all the job will be done.
     *
     * @return null
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
