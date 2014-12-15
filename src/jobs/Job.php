<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains job class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.jobs
 */

namespace Yiiq\jobs;

use Yiiq\Yiiq;
use Yiiq\base\JobComponent;

/**
 * Job class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 *
 * @property Metadata $metadata
 * @property-read Status $status
 * @property-read Result $result
 * @property-read Payload $payload
 */
class Job extends JobComponent
{
    /**
     * @var Metadata
     */
    protected $metadata = null;

    /**
     * @var Status
     */
    protected $status = null;

    /**
     * @var Result
     */
    protected $result = null;

    public function __toString()
    {
        return $this->getMetadata()->queue.':'.$this->id;
    }

    /**
     * Get metadata.
     * At first call metadata will be loaded from redis.
     *
     * @return Metadata
     */
    public function getMetadata()
    {
        if ($this->metadata === null) {
            $this->metadata = new Metadata($this->owner, $this->id);
            $this->metadata->refresh();
        }

        return $this->metadata;
    }

    /**
     * Set metadata.
     *
     * @param Metadata $metadata
     */
    public function setMetadata(Metadata $metadata)
    {
        $this->metadata = $metadata;
    }

    /**
     * @return Status
     */
    public function getStatus()
    {
        if ($this->status === null) {
            $this->status = new Status($this->owner, $this->id);
        }

        return $this->status;
    }

    /**
     * @return Result
     */
    public function getResult()
    {
        if ($this->result === null) {
            $this->result = new Result($this->owner, $this->id);
        }

        return $this->result;
    }

    /**
     * Get payload object.
     *
     * @return \Yiiq\jobs\Payload
     */
    public function getPayload()
    {
        $metadata = $this->getMetadata();
        $class = $metadata->class;

        return new $class($this);
    }
}
