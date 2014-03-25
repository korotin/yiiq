<?php
require_once __DIR__.'/YiiqBaseCommand.php';

class YiiqCommand extends YiiqBaseCommand {
    
    public function actionStart($queue = null)
    {
        $command = 'nohup sh -c "'.escapeshellarg(Yii::app()->basePath.'/yiic').' yiiqWorker start --queue='.$queue.'" > /dev/null 2>&1 &';
        $return = null;
        echo "Running worker... ";
        exec($command, $return);
        echo "Done.\n";
    }

    public function actionStop()
    {
        $pids = Yii::app()->yiiq->pidPool->getData();
        if ($pids) {
            foreach ($pids as $pid) {
                echo "Killing $pid...\n";
                posix_kill($pid, SIGTERM);
            }
            echo "Done.\n";
        }
        else {
            echo "No pids found.\n";
        }
    }
    
}