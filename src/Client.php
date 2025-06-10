<?php

namespace Webman\Sequency;

use support\Redis;
use Throwable;

class Client
{
  // Constants
  const string QUEUE_WAITING_PREFIX = '{sequency}-waiting:';
  const string QUEUE_DELAYED_KEY = '{sequency}-delayed';
  const string QUEUE_FAILED_KEY = '{sequency}-failed';
  const string QUEUE_TASK_INDEX_KEY = '{sequency}-tasks';
  const int MAX_INPUT_PRIORITY = 999;
  const int MIN_INPUT_PRIORITY = 0;

  /**
   * Calculates a lexicographically sortable score for a job.
   */
  public static function calculatePriorityScore(int $userPriority, int $timestamp): string
  {
    $normalizedUserPriority = max(self::MIN_INPUT_PRIORITY, min(self::MAX_INPUT_PRIORITY, $userPriority));
    return sprintf('%0' . strlen(self::MAX_INPUT_PRIORITY) . 'd%010d', self::MAX_INPUT_PRIORITY - $normalizedUserPriority, $timestamp);
  }

  /**
   * Send to a specific connection.
   */
  public static function sendTo(string $connectionName, string $queueName, mixed $data, int $delay = 0, int $priority = 0, ?string $taskId = null): string|false
  {
    $now = time();
    $taskId = $taskId ?? uuid_create();
    $packageStr = igbinary_serialize([
      'id' => $taskId,
      'time' => $now,
      'delay' => $delay,
      'attempts' => 0,
      'queue' => $queueName,
      'priority' => $priority,
      'data' => $data
    ]);
    try {
      $redis = Redis::connection($connectionName);
      $redis->multi();
      $redis->hSet(self::QUEUE_TASK_INDEX_KEY, $taskId, $packageStr);
      if ($delay > 0) {
        $redis->zAdd(self::QUEUE_DELAYED_KEY, $now + $delay, $packageStr);
      } else {
        $score = self::calculatePriorityScore($priority, $now);
        $redis->zAdd(self::QUEUE_WAITING_PREFIX . $queueName, $score, $packageStr);
      }
      $result = $redis->exec();
      if ($result && !in_array(false, $result, true)) {
        return $taskId;
      }
      $redis->hDel(self::QUEUE_TASK_INDEX_KEY, $taskId);
      return false;
    } catch (Throwable) {
      return false;
    }
  }

  /**
   * Send a message using the default connection.
   */
  public static function send(string $queueName, mixed $data, int $delay = 0, int $priority = 0, ?string $taskId = null): string|false
  {
    return static::sendTo('default', $queueName, $data, $delay, $priority, $taskId);
  }

  /**
   * Cancel a message from a sequency queue on a specific connection.
   * The specific queue name is not needed as the task is looked up by its global ID.
   *
   * @param string $connectionName
   * @param string $taskId The ID of the task to cancel.
   * @return bool
   * @throws Throwable
   */
  public static function cancelFrom(string $connectionName, string $taskId): bool
  {
    $redis = Redis::connection($connectionName);
    $packageStr = $redis->hGet(self::QUEUE_TASK_INDEX_KEY, $taskId);
    if (empty($packageStr)) {
      return false; // Task does not exist.
    }
    $task = igbinary_unserialize($packageStr);
    if (!is_array($task) || !isset($task['delay'], $task['queue'])) {
      // Invalid task data, clean it up.
      $redis->hDel(self::QUEUE_TASK_INDEX_KEY, $taskId);
      return false;
    }
    // Determine which queue the task is in based on its state.
    $isDelayed = ($task['delay'] > 0 && $task['attempts'] === 0) || ($task['attempts'] > 0);
    $queueKey = $isDelayed ? self::QUEUE_DELAYED_KEY : self::QUEUE_WAITING_PREFIX . $task['queue'];
    $redis->multi();
    $redis->zRem($queueKey, $packageStr);
    $redis->hDel(self::QUEUE_TASK_INDEX_KEY, $taskId);
    $result = $redis->exec();
    // zRem result is at index 0. It returns the number of elements removed (1 if successful).
    return isset($result[0]) && $result[0] > 0;
  }

  /**
   * Cancel a message from a sequency queue on the default connection.
   *
   * @param string $taskId The ID of the task to cancel.
   * @return bool
   * @throws Throwable
   */
  public static function cancel(string $taskId): bool
  {
    return static::cancelFrom('default', $taskId);
  }
}