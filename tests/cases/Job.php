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

abstract class Job extends Base
{
    const TIME_FOR_JOB  = 900000;

    public function startParametersProvider()
    {
        return [
            /*['default_'.TEST_TOKEN, 1],
            ['default_'.TEST_TOKEN, 5],
            ['default_'.TEST_TOKEN, 10],*/
            ['default_'.TEST_TOKEN, 15],
        ];
    }

    public function badClassProvider()
    {
        return [
            ['\Yiiq\tests\jobs\BadJob'], 
            //'\Yiiq\tests\jobs\YiiqBadJob2'],
            //['\Yiiq\tests\jobs\YiiqBadJob3'],
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
        $timeForJob = self::TIME_FOR_JOB + ($bad ? array_sum(\Yii::app()->yiiq->faultIntervals) * (1000000 + self::TIME_FOR_JOB) : 0);
        $timeForAllJobs = ceil(($jobs * $timeForJob) / $threads);
        if ($timeForAllJobs < $timeForJob) {
            $timeForAllJobs = $timeForJob;
        }
        usleep($timeForAllJobs);
    }
}