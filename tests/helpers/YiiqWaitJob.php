<?php

class YiiqWaitJob extends YiiqBaseJob
{
    public $sleep;

    public function run()
    {
        sleep($this->sleep);
    }
}