<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains good job class.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.jobs
 */

namespace Yiiq\tests\jobs;

class GoodJob extends \Yiiq\jobs\Base
{
    public $file = 'goodjob';
    public $content = 'done';

    public function run()
    {
        $path = \Yii::getPathOfAlias('application.runtime').DIRECTORY_SEPARATOR.$this->file;
        if (file_exists($path)) {
            $prepend = file_get_contents($path);
        } else {
            $prepend = '';
        }
        file_put_contents($path, $prepend.$this->content);
    }
}
