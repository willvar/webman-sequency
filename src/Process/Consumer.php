<?php

namespace Webman\Sequency\Process;

use FilesystemIterator;
use Psr\Log\LoggerInterface;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use support\Container;
use support\Log;
use Webman\Sequency\Consumer as ConsumerInterface;
use Webman\Sequency\NonRetryableException;
use Webman\Sequency\RedisConnection;
use Workerman\Redis\Client as AsyncRedisClient;
use Workerman\Timer;
use Throwable;

class Consumer
{
  protected string $_consumerDir = '';
  /**
   * @var ConsumerInterface[] Stores ConsumerInterface instances keyed by their redisQueueKey
   */
  protected array $_consumers = [];
  protected ?LoggerInterface $_logger = null;
  protected ?int $_delayedQueueTimerId = null;
  /** @var AsyncRedisClient|null */
  protected ?AsyncRedisClient $_listenerClient = null;
  /** @var AsyncRedisClient|null */
  protected ?AsyncRedisClient $_commandClient = null;
  /** @var string[] */
  protected array $_queueKeys = [];

  public function __construct($consumer_dir = '', $loggerChannel = 'plugin.webman.sequency.default')
  {
    $this->_consumerDir = $consumer_dir;
    if (class_exists(Log::class) && $loggerChannel) {
      try {
        /** @var LoggerInterface $loggerInstance */
        $loggerInstance = Log::channel($loggerChannel);
        $this->_logger = $loggerInstance;
      } catch (Throwable $e) {
        echo "Error initializing logger for Sequency: " . $e->getMessage() . "\n";
      }
    }
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
      $consumerInstance = Container::get($class);
      if (!property_exists($consumerInstance, 'queue') || empty($consumerInstance->queue)) {
        continue;
      }
      $queueName = $consumerInstance->queue;
      $redisQueueKey = RedisConnection::QUEUE_WAITING_PREFIX . $queueName;
      $this->_consumers[$redisQueueKey] = $consumerInstance;
    }
    if (empty($this->_consumers)) {
      return;
    }
    $this->_queueKeys = array_keys($this->_consumers);
    $configs = config('sequency', config('plugin.webman.sequency.redis', []));
    $connectionName = 'default';
    if (!isset($configs[$connectionName])) {
      $this->log("Error: '$connectionName' Redis connection not found in sequency config.");
      return;
    }
    $config = $configs[$connectionName];
    $address = $config['host'] ?? 'redis://127.0.0.1:6379';
    $options = $config['options'] ?? [];
    $this->_listenerClient = new AsyncRedisClient($address, $options);
    $this->_commandClient = new AsyncRedisClient($address, $options);
    $on_auth_success = function ($success) use ($options) {
      if ($success && !empty($options['db'])) {
        $this->_listenerClient->select($options['db']);
        $this->_commandClient->select($options['db']);
      }
    };
    if (!empty($options['auth'])) {
      $this->_listenerClient->auth($options['auth'], $on_auth_success);
      $this->_commandClient->auth($options['auth']);
    } else {
      $on_auth_success(true);
    }
    $this->listenForJobs();
    if ($this->_delayedQueueTimerId) {
      Timer::del($this->_delayedQueueTimerId);
    }
    $this->_delayedQueueTimerId = Timer::add(1, function () {
      $this->_commandClient->zRangeByScore(RedisConnection::QUEUE_DELAYED_KEY, '-inf', time(), [], function ($messages) {
        if (empty($messages)) {
          return;
        }
        foreach ($messages as $packedMessage) {
          $this->_commandClient->zRem(RedisConnection::QUEUE_DELAYED_KEY, $packedMessage, function ($remResult) use ($packedMessage) {
            if (!$remResult) return;
            $msg = json_decode_lite($packedMessage, true);
            if (is_array($msg) && isset($msg['queue'], $msg['time'])) {
              $targetQueue = RedisConnection::QUEUE_WAITING_PREFIX . $msg['queue'];
              $score = RedisConnection::calculatePriorityScore((int)($msg['priority'] ?? 0), (int)$msg['time']);
              $this->_commandClient->zAdd($targetQueue, $score, $packedMessage);
            } else {
              $this->failMessage($packedMessage, "Invalid format from delayed queue");
            }
          });
        }
      });
    });
  }

  protected function listenForJobs(): void
  {
    $this->_listenerClient->bzPopMin($this->_queueKeys, 0, function ($result) {
      // 当 bzPopMin 返回结果后，此回调函数才会被执行
      if ($result) {
        $redisQueueKey = $result[0];
        $packedMessage = $result[1];
        $consumerInstance = $this->_consumers[$redisQueueKey] ?? null;
        if ($consumerInstance) {
          $message = json_decode_lite($packedMessage, true);
          if (!is_array($message) || !isset($message['data'], $message['attempts'])) {
            $this->failMessage($packedMessage, "Invalid message format");
            return;
          }
          $connectionName = property_exists($consumerInstance, 'connection') && !empty($consumerInstance->connection) ? $consumerInstance->connection : 'default';
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
            $this->failMessage($packedMessage, $e->getMessage(), $consumerInstance, $message['data'], $e);
          } catch (Throwable $e) {
            $message['attempts'] = $message['attempts'] + 1;
            if ($message['attempts'] >= $maxAttempts) {
              $this->failMessage(json_encode_lite($message), $e->getMessage(), $consumerInstance, $message['data'], $e);
            } else {
              $delay = $retrySeconds * pow(2, $message['attempts'] - 1);
              $retryAt = time() + $delay;
              $this->_commandClient->zAdd(RedisConnection::QUEUE_DELAYED_KEY, $retryAt, json_encode_lite($message));
            }
          }
        }
      }
      $this->listenForJobs();
    });
  }

  protected function failMessage(string $packedMessageOriginal, string $errorMessage, ?ConsumerInterface $consumer = null, $data = null, ?Throwable $exception = null): void
  {
    $message = json_decode_lite($packedMessageOriginal, true);
    $packageToStore = json_encode_lite([
      'original_payload' => is_array($message) ? $message : $packedMessageOriginal,
      'error' => $errorMessage,
      'failed_at' => date('Y-m-d H:i:s')
    ]);
    $jobId = $message['id'] ?? 'unknown';
    $this->_commandClient->lPush(RedisConnection::QUEUE_FAILED_KEY, $packageToStore);
    if ($consumer && method_exists($consumer, 'onFail') && $data !== null && $exception !== null) {
      try {
        $consumer->onFail($data, $exception);
      } catch (Throwable $e) {
        $this->log("Error in consumer's onFail method for job ID $jobId: " . $e->getMessage());
      }
    }
  }

  protected function log(string $message): void
  {
    if ($this->_logger) {
      $this->_logger->info($message);
    } else {
      echo date('Y-m-d H:i:s') . ' [Sequency] ' . $message . "\n";
    }
  }

  public function onWorkerStop(): void
  {
    $this->_listenerClient?->close();
    $this->_commandClient?->close();
    if ($this->_delayedQueueTimerId) {
      Timer::del($this->_delayedQueueTimerId);
      $this->_delayedQueueTimerId = null;
    }
  }
}