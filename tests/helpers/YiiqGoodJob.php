<?php

class YiiqGoodJob extends YiiqBaseJob
{

    public $file = 'goodjob';
    public $content = 'done';

    public function run()
    {
        $path = Yii::getPathOfAlias('application.runtime').DIRECTORY_SEPARATOR.$this->file;
        if (file_exists($path)) {
            $prepend = file_get_contents($path);
        }
        else {
            $prepend = '';
        }
        file_put_contents($path, $prepend.$this->content);
    }

}