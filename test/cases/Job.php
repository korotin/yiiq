<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains abstract test case for jobs.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.cases
 */

namespace Yiiq\test\cases;

abstract class Job extends Base
{
    const TIME_FOR_JOB  = 1800000;

    public function startParametersProvider()
    {
        return [
            ['default_'.TEST_TOKEN, 1],
            ['default_'.TEST_TOKEN, 2],
            ['default_'.TEST_TOKEN, 4],
            ['default_'.TEST_TOKEN, 6],
        ];
    }

    public function badClassProvider()
    {
        return [
            ['\Yiiq\test\jobs\BadJob'],
            ['\Yiiq\test\jobs\BadJob2'],
        ];
    }

    public function queuesThreadsJobsProvider()
    {
        return [
            [1, 1, 1],
            [1, 2, 2],
            [1, 2, 4],
            [2, 1, 1],
            [2, 2, 2],
            [2, 2, 4],
            [4, 1, 1],
            [4, 2, 2],
            [4, 2, 4],
        ];
    }

    public function startParametersAndBadClassProvider()
    {
        $parameters = $this->startParametersProvider();
        $badClasses = $this->badClassProvider();

        $data = [];
        foreach ($parameters as $parameter) {
            foreach ($badClasses as $badClass) {
                $data[] = array_merge($parameter, $badClass);
            }
        }

        return $data;
    }

    protected function waitForJobs($threads, $jobs, $bad = false)
    {
        $timeForJob =
            self::TIME_FOR_JOB
            + (
                $bad
                    ? array_sum(\Yii::app()->yiiq->faultIntervals) * (1000000 + self::TIME_FOR_JOB)
                    : 0
            );
        $timeForAllJobs = ceil(($jobs * $timeForJob) / $threads);
        if ($timeForAllJobs < $timeForJob) {
            $timeForAllJobs = $timeForJob;
        }
        usleep($timeForAllJobs);
    }
}
