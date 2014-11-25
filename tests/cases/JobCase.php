<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains abstract test case for jobs.
 * 
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.cases
 */

namespace Yiiq\tests\cases;

abstract class JobCase extends TestCase
{
    public function startParametersProvider()
    {
        return [
            ['default', 1],
            ['default', 5],
            ['default', 10],
            ['default', 15],
            ['custom', 1],
            ['custom', 5],
            ['custom', 10],
            ['custom', 15],
        ];
    }

    public function badClassProvider()
    {
        return [
            ['\Yiiq\tests\jobs\BadJob'], 
            ['\Yiiq\tests\jobs\YiiqBadJob2'],
            ['\Yiiq\tests\jobs\YiiqBadJob3'],
        ];
    }

    public function startParametersAndBadClassProvider()
    {
        $parameters = $this->startParametersProvider();
        $badClasses = $this->badClassProvider();

        $data = [];
        foreach ($parameters as $parameter){
            foreach ($badClasses as $badClass) {
                $data[] = array_merge($parameter, $badClass);
            }
        }

        return $data;
    }

    protected function waitForJobs($threads, $jobs, $bad = false)
    {
        $timeForJob = 400000 + ($bad ? array_sum(\Yii::app()->yiiq->faultIntervals) * 1000000 * 1.4 : 0);
        $timeForAllJobs = ceil(($jobs * $timeForJob) / $threads);
        if ($timeForAllJobs < $timeForJob) {
            $timeForAllJobs = $timeForJob;
        }
        usleep($timeForAllJobs);
    }
}