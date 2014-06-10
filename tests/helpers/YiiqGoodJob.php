<?php

class YiiqGoodJob extends YiiqBaseJob
{

    public $file = 'goodjob';
    public $content = 'done';

    public function run()
    {
        $path = Yii::getPathOfAlias('application.runtime').DIRECTORY_SEPARATOR.$this->file;
        file_put_contents($path, $this->content);
    }

}