<?php

namespace Simpleue\Queue;

use Pheanstalk\Job;
use Pheanstalk\Pheanstalk;
use Simpleue\Locker\BaseLocker;

/**
 * Class BeanstalkdQueue
 * @author Adeyemi Olaoye <yemexx1@gmail.com>
 * @package Simpleue\Queue
 */
class BeanStalkdQueue implements Queue
{
    /** @var  Pheanstalk */
    private $beanStalkdClient;
    private $sourceQueue;
    private $failedQueue;
    private $errorQueue;
    private $timeout;
    /**
     * @var BaseLocker
     */
    private $locker;

    public function __construct($beanStalkdClient, $queueName, $timeout = 30)
    {
        $this->beanStalkdClient = $beanStalkdClient;
        $this->setQueues($queueName);
        $this->timeout = $timeout;
    }

    protected function setQueues($queueName)
    {
        $this->sourceQueue = $queueName;
        $this->failedQueue = $queueName . '-failed';
        $this->errorQueue = $queueName . '-error';
    }

    /**
     * @param BaseLocker $locker
     */
    public function setLocker($locker)
    {
        $this->locker = $locker;
    }

    public function getNext()
    {
        $this->beanStalkdClient->watch($this->sourceQueue);
        $job = $this->beanStalkdClient->reserveWithTimeout($this->timeout);
        if (!$job) {
            return [false, $job];
        }
        try {
            if ($this->locker && $this->locker->lock($job->getData()) === false) {
                return [false, $job];
            }
        } catch (\Exception $e) {
            return [false, $job];
        }

        return [true, $job];
    }

    public function successful($job)
    {
        return $this->delete($job);
    }

    private function delete($job)
    {
        try {
            if ($job) {
                $this->beanStalkdClient->delete($job);
            }
        } catch (\Exception $e) {
        }
        return true;
    }

    /**
     * @param $job Job
     * @return int
     */
    public function failed($job)
    {
        $this->beanStalkdClient->useTube($this->failedQueue);
        $this->beanStalkdClient->put($job->getData());
        $this->delete($job);
    }

    /**
     * @param $job Job
     * @return int
     */
    public function error($job)
    {
        $this->beanStalkdClient->useTube($this->errorQueue);
        $this->beanStalkdClient->put($job->getData());
        $this->delete($job);
    }

    public function nothingToDo()
    {
        return;
    }

    public function stopped($job)
    {
        return $this->delete($job);
    }

    /**
     * @param $job Job
     * @return string
     */
    public function getMessageBody($job)
    {
        return $job->getData();
    }

    /**
     * @param $job Job
     * @return string
     */
    public function toString($job)
    {
        return json_encode(['id' => $job->getId(), 'data' => $job->getData()]);
    }

    /**
     * @param $job string
     * @return int
     */
    public function sendJob($job)
    {
        $this->beanStalkdClient->useTube($this->sourceQueue);
        return $this->beanStalkdClient->put($job->getData());
    }
}
