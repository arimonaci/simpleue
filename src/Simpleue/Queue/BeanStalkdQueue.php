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
        $job = $this->beanStalkdClient->reserve($this->timeout);
        if ($this->locker && $this->locker->lock($job) === false) {
            throw new \RuntimeException(
                'Beanstalkd msg lock cannot acquired!'
                . ' LockId: ' . $this->locker->getJobUniqId($job)
                . ' LockerInfo: ' . $this->locker->getLockerInfo()
            );
        }
        return $job;
    }

    public function successful($job)
    {
        return $this->delete($job);
    }

    private function delete($job)
    {
        try {
            $this->beanStalkdClient->delete($job);
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
        $this->beanStalkdClient->putInTube($this->failedQueue, $job->getData());
        $this->delete($job);
    }

    /**
     * @param $job Job
     * @return int
     */
    public function error($job)
    {
        $this->beanStalkdClient->putInTube($this->errorQueue, $job->getData());
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
        return $this->beanStalkdClient->putInTube($this->sourceQueue, $job);
    }
}
