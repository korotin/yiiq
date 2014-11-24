<?php

class YiiqReturnJob extends YiiqBaseJob
{
    
    public $result;

    public function run()
    {
        return $this->result;
    }

}