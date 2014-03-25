<?php

abstract class YiiqBaseJob {
    
    protected $queue;

    abstract protected function run();

    public function execute($args)
    {
        foreach ($args as $k => $v) {
            $this->$k = $v;
        }

        $this->run();
    }

}