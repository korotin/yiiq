<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains Yiiq dummy job class.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package ext.yiiq.jobs
 */

/**
 * Yiiq dummy job class.
 * Dummy job may be used for tests. It emulates long time execution. 
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class YiiqDummyJob extends YiiqBaseJob
{
    /**
     * Time to wait before job completes.
     * @var integer
     */
    public $sleep = 10;

    public function run()
    {
        Yii::trace('Started dummy job '.$this->queue.':'.$this->id.' (sleep for '.$this->sleep.'s).');
        sleep($this->sleep);
        Yii::trace('Job '.$this->queue.':'.$this->id.' completed.');
    }

}