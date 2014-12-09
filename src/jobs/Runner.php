<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains job runner class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.commands
 */

namespace Yiiq\jobs;

use Yiiq\Yiiq;
use Yiiq\base\Component;

/**
 * Job runner class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class Runner extends Component
{
    /**
     * @var Job
     */
    protected $job = null;

    public function __construct(Yiiq $owner, Job $job)
    {
        parent::__construct($owner);
        $this->job = $job;
    }

    public function run()
    {
        $job = $this->job;

        // Try to fork process.
        $childPid = pcntl_fork();

        // Force reconnect to redis for parent and child due to bug in PhpRedis
        // (https://github.com/nicolasff/phpredis/issues/474).
        \Yii::app()->redis->getClient(true);

        if ($childPid > 0) {
            return $childPid;
        } elseif ($childPid < 0) {
            // If we're failed to fork process, restore job and exit.
            \Yii::app()->yiiq->restore($job->id);

            return;
        }

        // We are child - get our pid.
        $childPid = posix_getpid();

        $this->owner->setProcessTitle(
            'job',
            $job->metadata->queue,
            'initializing'
        );

        \Yii::trace('Starting job '.$job->metadata->queue.':'.$job->id.' ('.$job->metadata->class.')...');
        $this->owner->setProcessTitle(
            'job',
            $job->metadata->queue,
            'executing '.$job->metadata->id.' ('.$job->metadata->class.')'
        );

        $job->status->markAsStarted($childPid);

        $payload = $job->payload;
        $result = $payload->execute($job->metadata->args);

        if ($job->metadata->type === Yiiq::TYPE_REPEATABLE) {
            $job->status->markAsStopped();
        } else {
            $job->metadata->delete();
            $job->result->save($result);
            $job->status->markAsCompleted();
        }

        \Yii::trace('Job '.$job->metadata->queue.':'.$job->metadata->id.' done.');

        exit(0);
    }
}
