<?php

namespace Webman\Sequency;

use support\Redis;
use Throwable;

/**
 * Sequency producer client that uses the official webman/redis component.
 */
class Client
{
  /**
   * Send a message to a sequency queue using the default connection.
   * @param string $queueName
   * @param mixed $data
   * @param int $delay
   * @param int|null $priority
   * @return bool
   * @throws Throwable
   */
  public static function send(string $queueName, mixed $data, int $delay = 0, ?int $priority = null): bool
  {
    return self::sendMessage('default', $queueName, $data, $delay, $priority);
  }

  /**
   * Send a message to a sequency queue using a specific connection.
   * @param string $connectionName
   * @param string $queueName
   * @param mixed $data
   * @param int $delay
   * @param int|null $priority
   * @return bool
   * @throws Throwable
   */
  public static function sendTo(string $connectionName, string $queueName, mixed $data, int $delay = 0, ?int $priority = null): bool
  {
    return self::sendMessage($connectionName, $queueName, $data, $delay, $priority);
  }

  /**
   * The core logic for sending a message.
   * @throws Throwable
   */
  private static function sendMessage(string $connection, string $queueName, mixed $data, int $delay, ?int $priority): bool
  {
    $priority = $priority ?? 0;
    $now = time();
    $packageStr = json_encode_lite([
      'id' => uuid_create(),
      'time' => $now,
      'delay' => $delay,
      'attempts' => 0,
      'queue' => $queueName,
      'priority' => $priority,
      'data' => $data
    ]);
    $redis = Redis::connection($connection);
    if ($delay > 0) {
      return (bool)$redis->zAdd(RedisConnection::QUEUE_DELAYED_KEY, $now + $delay, $packageStr);
    }
    $score = RedisConnection::calculatePriorityScore($priority, $now);
    return (bool)$redis->zAdd(RedisConnection::QUEUE_WAITING_PREFIX . $queueName, $score, $packageStr);
  }
}