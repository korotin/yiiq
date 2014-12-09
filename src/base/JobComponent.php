<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains job component class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.base
 */

namespace Yiiq\base;

use Yiiq\Yiiq;

/**
 * Job component class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 *
 * @property-read string $id
 */
abstract class JobComponent extends Component
{
    /**
     * Job id.
     *
     * @var string
     */
    protected $id = null;

    public function __construct(Yiiq $owner, $id)
    {
        if (!$id) {
            throw new \CException('Job id cannot be empty.');
        }

        parent::__construct($owner);
        $this->id = $id;
    }

    public function getId()
    {
        return $this->id;
    }
}
