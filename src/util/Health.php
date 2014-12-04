<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains Yiiq health checker class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.util
 */

namespace Yiiq\util;

use Yiiq\Yiiq;
use Yiiq\base\Component;

/**
 * Yiiq health checker class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class Health extends Component
{
    /**
     * Is process with given pid alive?
     *
     * @param  int     $pid
     * @return boolean
     */
    public function isPidAlive($pid)
    {
        return posix_kill($pid, 0);
    }

    /**
     * Remove dead process pids from redis set.
     *
     * @param  \ARedisSet $pool
     * @return integer    amount of dead pids
     */
    protected function checkPidPool(\ARedisSet $pool)
    {
        $removed = 0;
        $pids = $pool->getData(true);
        foreach ($pids as $pid) {
            if ($this->isPidAlive($pid)) {
                continue;
            }
            $pool->remove($pid);
            $removed++;
        }

        return $removed;
    }

    /**
     * Check for dead children.
     *
     * @param  bool $log
     * @return int  dead children count
     */
    protected function checkForDeadChildren($log)
    {
        if ($log) {
            echo "Checking for dead children... ";
        }

        $deadChildren = 0;
        $keys = \Yii::app()->redis->keys($this->owner->prefix.':children:*');
        foreach ($keys as $key) {
            if (\Yii::app()->redis->prefix) {
                $key = mb_substr($key, mb_strlen(\Yii::app()->redis->prefix));
            }
            $deadChildren += $this->checkPidPool(new \ARedisSet($key));
        }
        if ($log) {
            echo "$deadChildren found.\n";
        }

        return $deadChildren;
    }

    /**
     * Check for dead workers.
     *
     * @param  bool $log
     * @return int  dead workers count
     */
    protected function checkForDeadWorkers($log)
    {
        if ($log) {
            echo "Checking for dead workers... ";
        }
        $deadWorkers = $this->checkPidPool($this->owner->pools->pids);
        if ($log) {
            echo "$deadWorkers found.\n";
        }

        return $deadWorkers;
    }

    /**
     * Check for stopped jobs.
     *
     * @param  bool      $log
     * @return integer[] [stopped jobs, restored jobs]
     */
    protected function checkForStoppedJobs($log)
    {
        if ($log) {
            echo "Checking for stopped jobs... ";
        }

        $pool = $this->owner->pools->executing;

        $stopped = 0;
        $restored = 0;
        $jobs = $pool->getData(true);
        foreach ($jobs as $id => $pid) {
            if ($this->isPidAlive($pid)) {
                continue;
            }
            $stopped++;
            if ($this->owner->restoreJob($id)) {
                $restored++;
            }
        }

        if ($log) {
            echo "$stopped found, $restored restored.\n";
        }

        return [$stopped, $restored];
    }

    /**
     * Check redis db consistency.
     *
     * Remove dead worker pids from pid pool, remove lost jobs
     * and restore stopped jobs.
     *
     * @param  bool $log (optional)
     * @return bool true if no errors found
     */
    public function check($log = true)
    {
        $deadChildren = $this->checkForDeadChildren($log);
        $deadWorkers = $this->checkForDeadWorkers($log);
        list($stoppedJobs, $restoredJobs) = $this->checkForStoppedJobs($log);

        return
            $deadChildren === 0
            && $deadWorkers === 0
            && $stoppedJobs === 0
            && $restoredJobs === 0;
    }
}
