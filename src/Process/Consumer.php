<?php

namespace Webman\Sequency\Process;

use FilesystemIterator;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use support\Container;
use Webman\Sequency\Redis;
use Webman\Sequency\RedisConnection;
use Webman\Sequency\Consumer as ConsumerInterface;
use Webman\Sequency\NonRetryableException;
use Workerman\Timer;
use Throwable;

class Consumer
{
  protected string $_consumerDir = '';
  /**
   * @var ConsumerInterface[] Stores ConsumerInterface instances keyed by their redisQueueKey
   */
  protected array $_consumers = [];
  protected ?int $_delayedQueueTimerId = null;
  /**
   * @var array Stores pull timer IDs keyed by their redisQueueKey
   */
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
    $dir_iterator = new RecursiveDirectoryIterator($this->_consumerDir, FilesystemIterator::SKIP_DOTS);
    $iterator = new RecursiveIteratorIterator($dir_iterator);
    foreach ($iterator as $file) {
      if ($file->isDir() || $file->getExtension() !== 'php') {
        continue;
      }
      $relativePath = str_replace(base_path() . '/', '', $file->getPathname());
      $class = str_replace(['/', '.php'], ['\\', ''], $relativePath);
      if (str_starts_with($file->getPathname(), app_path())) {
        $class = 'app\\' . str_replace(['/', '.php'], ['\\', ''], substr($file->getPathname(), strlen(app_path() . '/')));
      }
      if (!is_a($class, ConsumerInterface::class, true)) {
        continue;
      }
      try {
        /** @var ConsumerInterface $consumerInstance */
        $consumerInstance = Container::get($class);
        // 消费者类必须定义 public $queue 属性
        if (!property_exists($consumerInstance, 'queue') || empty($consumerInstance->queue)) {
          continue;
        }
        $queueName = $consumerInstance->queue;
        $redisQueueKey = RedisConnection::QUEUE_WAITING_PREFIX . $queueName;
        $this->_consumers[$redisQueueKey] = $consumerInstance;
      } catch (Throwable) {
      }
    }
    foreach ($this->_consumers as $redisQueueKey => $consumerInstance) {
      $this->_pullTimerIds[$redisQueueKey] = Timer::add(0.001, function () use ($consumerInstance, $redisQueueKey) {
        $this->tryConsume($consumerInstance, $redisQueueKey);
      });
    }
    if ($this->_delayedQueueTimerId) {
      Timer::del($this->_delayedQueueTimerId);
    }
    $this->_delayedQueueTimerId = Timer::add(1, function () {
      try {
        // 考虑使 'default' 连接可配置，或从某个主消费者/全局配置获取
        $defaultConnectionForDelayed = 'default'; // 或更智能的逻辑
        $allConnections = config('sequency', config('plugin.webman.sequency.redis', []));
        if (!isset($allConnections[$defaultConnectionForDelayed])) {
          //尝试获取第一个可用的连接配置作为延迟队列处理器连接
          if (!empty($allConnections)) {
            reset($allConnections);
            $defaultConnectionForDelayed = key($allConnections);
          } else {
            return;
          }
        }
        $redis = Redis::connection($defaultConnectionForDelayed);
        $now = time();
        $messagesToProcess = $redis->execCommand('zRangeByScore', RedisConnection::QUEUE_DELAYED_KEY, '-inf', (string)$now, ['LIMIT', 0, 100]);
        if (empty($messagesToProcess)) {
          return;
        }
        foreach ($messagesToProcess as $packedMessage) {
          if ($redis->execCommand('zRem', RedisConnection::QUEUE_DELAYED_KEY, $packedMessage)) {
            $messageData = igbinary_unserialize($packedMessage);
            if (is_array($messageData) && isset($messageData['queue'], $messageData['time'])) {
              $targetWaitingQueue = RedisConnection::QUEUE_WAITING_PREFIX . $messageData['queue'];
              // 查找对应的消费者实例以确定连接 (如果需要为每个队列使用不同连接处理延迟任务的转移)
              // 简单起见，这里仍然使用 $defaultConnectionForDelayed 进行 zAdd
              // 但如果目标队列的消费者在不同连接上，理论上应该用那个连接
              $priority = (int)($messageData['priority'] ?? 0);
              $originalEnqueueTime = (int)$messageData['time'];
              $score = RedisConnection::calculatePriorityScore($priority, $originalEnqueueTime);
              // 假设所有等待队列都在同一个Redis实例上，或者 $redis 可以处理所有键
              $redis->execCommand('zAdd', $targetWaitingQueue, $score, $packedMessage);
            } else {
              $this->failMessage($packedMessage, $redis, "Invalid format or missing queue/priority/time from delayed queue");
            }
          }
        }
      } catch (Throwable) {
      }
    });
  }

  protected function tryConsume(ConsumerInterface $consumerInstance, string $redisQueueKey): void
  {
    try {
      // 从消费者实例或全局配置中获取 maxAttempts 和 retrySeconds
      $connectionName = property_exists($consumerInstance, 'connection') && !empty($consumerInstance->connection) ? $consumerInstance->connection : 'default';
      $redis = Redis::connection($connectionName);
      $result = $redis->execCommand('zPopMin', $redisQueueKey, 1);
      if (is_array($result) && !empty($result)) {
        $packedMessage = key($result);
        $message = igbinary_unserialize($packedMessage);
        if (!is_array($message) || !isset($message['data']) || !isset($message['attempts'])) {
          $this->failMessage($packedMessage, $redis, "Invalid message format");
          return;
        }
        $globalRedisConfig = config('sequency', config('plugin.webman.sequency.redis', []))[$connectionName]['options'] ?? [];
        $maxAttempts = property_exists($consumerInstance, 'maxAttempts') && is_int($consumerInstance->maxAttempts)
          ? $consumerInstance->maxAttempts
          : ($globalRedisConfig['max_attempts'] ?? 5);
        $retrySeconds = property_exists($consumerInstance, 'retrySeconds') && is_int($consumerInstance->retrySeconds)
          ? $consumerInstance->retrySeconds
          : ($globalRedisConfig['retry_seconds'] ?? 5);
        try {
          $consumerInstance->consume($message['data']);
        } catch (NonRetryableException $e) {
          $this->failMessage($packedMessage, $redis, $e->getMessage(), $consumerInstance, $message['data'], $e);
        } catch (Throwable $e) {
          $message['attempts'] = ((int)$message['attempts'] ?? 0) + 1;
          if ($message['attempts'] >= $maxAttempts) {
            $this->failMessage(igbinary_serialize($message), $redis, $e->getMessage(), $consumerInstance, $message['data'], $e);
          } else {
            $delay = $retrySeconds * pow(2, $message['attempts'] - 1);
            $message['delay'] = $delay;
            $retryAt = time() + $delay;
            try {
              $redis->execCommand('zAdd', RedisConnection::QUEUE_DELAYED_KEY, $retryAt, igbinary_serialize($message));
            } catch (Throwable) {
            }
          }
        }
      }
    } catch (Throwable) {
      if (isset($this->_pullTimerIds[$redisQueueKey])) { // 使用 $redisQueueKey 来删除特定的定时器
        Timer::del($this->_pullTimerIds[$redisQueueKey]);
      }
      // 重新启动定时器时也传递 $redisQueueKey
      $this->_pullTimerIds[$redisQueueKey] = Timer::add(5, function () use ($consumerInstance, $redisQueueKey) {
        $this->tryConsume($consumerInstance, $redisQueueKey);
      });
    }
  }

  protected function failMessage(string $packedMessageOriginal, RedisConnection $redis, string $errorMessage, ?ConsumerInterface $consumer = null, $data = null, ?Throwable $exception = null): void
  {
    $message = igbinary_unserialize($packedMessageOriginal);
    if (is_array($message)) {
      $message['error'] = $errorMessage;
      $message['failed_at'] = date('Y-m-d H:i:s');
      $packedMessageToStore = igbinary_serialize($message);
    } else {
      $packedMessageToStore = igbinary_serialize([
        'original_payload' => $packedMessageOriginal,
        'error' => $errorMessage,
        'failed_at' => date('Y-m-d H:i:s')
      ]);
    }
    try {
      $redis->execCommand('lPush', RedisConnection::QUEUE_FAILED_KEY, $packedMessageToStore);
    } catch (Throwable) {
    }
    if ($consumer && method_exists($consumer, 'onFail') && $data !== null && $exception !== null) {
      try {
        $consumer->onFail($data, $exception);
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