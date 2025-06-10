<?php

namespace Webman\Sequency\Process;

use FilesystemIterator;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use support\Container;
use support\Redis;
use Webman\Sequency\Client;
use Webman\Sequency\Consumer as ConsumerInterface;
use Webman\Sequency\NonRetryableException;
use Workerman\Timer;
use Throwable;

class Consumer
{
  protected string $_consumerDir = '';
  protected array $_consumers = [];
  protected array $_queueToConsumerMap = [];
  protected ?int $_delayedQueueTimerId = null;
  protected array $_pullTimerIds = [];

  public function __construct($consumer_dir = '')
  {
    $this->_consumerDir = $consumer_dir;
  }

  public function onWorkerStart()
  {
    if (!is_dir($this->_consumerDir)) {
      return;
    }
    $iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($this->_consumerDir, FilesystemIterator::SKIP_DOTS));
    foreach ($iterator as $file) {
      if ($file->isDir() || $file->getExtension() !== 'php') {
        continue;
      }
      $class = str_replace([base_path() . '/', '/', '.php'], ['', '\\', ''], $file->getPathname());
      if (str_starts_with($class, 'app\\')) {
        $class = 'app\\' . substr($class, strlen('app/'));
      }
      if (!is_a($class, ConsumerInterface::class, true)) {
        continue;
      }
      try {
        $consumer = Container::get($class);
        if (empty($consumer->queue)) {
          continue;
        }
        $this->_consumers[Client::QUEUE_WAITING_PREFIX . $consumer->queue] = $consumer;
        $this->_queueToConsumerMap[$consumer->queue] = $consumer;
      } catch (Throwable) {
      }
    }
    foreach ($this->_consumers as $key => $consumer) {
      $this->_pullTimerIds[$key] = Timer::add(0.001, fn() => $this->tryConsume($consumer, $key));
    }
    if ($this->_delayedQueueTimerId) {
      Timer::del($this->_delayedQueueTimerId);
    }
    $this->_delayedQueueTimerId = Timer::add(1, function () {
      try {
        $redis = Redis::connection();
        $messages = $redis->zRangeByScore(Client::QUEUE_DELAYED_KEY, '-inf', (string)time(), ['LIMIT', 0, 100]);
        foreach ($messages as $packedMessage) {
          if ($redis->zRem(Client::QUEUE_DELAYED_KEY, $packedMessage)) {
            $msg = igbinary_unserialize($packedMessage);
            if (is_array($msg) && isset($msg['queue'], $msg['time'])) {
              $conn = $this->_queueToConsumerMap[$msg['queue']]->connection ?? 'default';
              $score = Client::calculatePriorityScore((int)($msg['priority'] ?? 0), (int)$msg['time']);
              Redis::connection($conn)->zAdd(Client::QUEUE_WAITING_PREFIX . $msg['queue'], $score, $packedMessage);
            } else {
              $this->failMessage($packedMessage, $redis, "Invalid format from delayed queue");
            }
          }
        }
      } catch (Throwable) {
      }
    });
  }

  protected function tryConsume(ConsumerInterface $consumer, string $redisQueueKey): void
  {
    try {
      $redis = Redis::connection($consumer->connection ?? 'default');
      $result = $redis->zPopMin($redisQueueKey, 1);
      if ($result) {
        $packedMessage = key($result);
        $message = igbinary_unserialize($packedMessage);
        if (!is_array($message) || !isset($message['data'], $message['attempts'], $message['id'])) {
          $this->failMessage($packedMessage, $redis, "Invalid message format");
          return;
        }
        try {
          $consumer->consume($message['data']);
          $redis->hDel(Client::QUEUE_TASK_INDEX_KEY, $message['id']);
        } catch (NonRetryableException $e) {
          $this->failMessage($packedMessage, $redis, $e->getMessage(), $consumer, $message['data'], $e);
        } catch (Throwable $e) {
          $message['attempts'] = ($message['attempts'] ?? 0) + 1;
          if ($message['attempts'] >= ($consumer->maxAttempts ?? 5)) {
            $this->failMessage(igbinary_serialize($message), $redis, $e->getMessage(), $consumer, $message['data'], $e);
          } else {
            $delay = ($consumer->retrySeconds ?? 5) * pow(2, $message['attempts'] - 1);
            $retryAt = time() + $delay;
            $updatedPackedMessage = igbinary_serialize($message);
            $retryRedis = Redis::connection();
            $retryRedis->multi();
            $retryRedis->hSet(Client::QUEUE_TASK_INDEX_KEY, $message['id'], $updatedPackedMessage);
            $retryRedis->zAdd(Client::QUEUE_DELAYED_KEY, $retryAt, $updatedPackedMessage);
            $retryRedis->exec();
          }
        }
      }
    } catch (Throwable) {
      if (isset($this->_pullTimerIds[$redisQueueKey])) Timer::del($this->_pullTimerIds[$redisQueueKey]);
      $this->_pullTimerIds[$redisQueueKey] = Timer::add(5, fn() => $this->tryConsume($consumer, $redisQueueKey));
    }
  }

  protected function failMessage(string $packedMsg, $redis, string $error, ?ConsumerInterface $consumer = null, $data = null, ?Throwable $e = null): void
  {
    $message = igbinary_unserialize($packedMsg);
    if (is_array($message)) {
      $message['error'] = $error;
      $message['failed_at'] = date('Y-m-d H:i:s');
      if (isset($message['id'])) {
        $redis->hDel(Client::QUEUE_TASK_INDEX_KEY, $message['id']);
      }
      $packedMsg = igbinary_serialize($message);
    }
    $redis->lPush(Client::QUEUE_FAILED_KEY, [$packedMsg]);
    if ($consumer && method_exists($consumer, 'onFail')) {
      try {
        $consumer->onFail($data, $e);
      } catch (Throwable) {
      }
    }
  }

  public function onWorkerStop(): void
  {
    foreach ($this->_pullTimerIds as $timerId) {
      if ($timerId) {
        Timer::del($timerId);
      }
    }
    $this->_pullTimerIds = [];
    if ($this->_delayedQueueTimerId) {
      Timer::del($this->_delayedQueueTimerId);
      $this->_delayedQueueTimerId = null;
    }
  }
}