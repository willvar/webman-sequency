<?php
namespace Webman\Sequency;

use Throwable;

class Client
{
  /**
   * @param string $queueName
   * @param mixed $data
   * @param int $delay
   * @param int $priority
   * @param string|null $taskId
   * @return string|false The task ID on success, false on failure.
   * @throws Throwable
   */
  public static function send(string $queueName, mixed $data, int $delay = 0, int $priority = 0, ?string $taskId = null): string|false
  {
    return Redis::connection()->send($queueName, $data, $delay, $priority, $taskId);
  }

  /**
   * Send to a specific connection.
   * @param string $connectionName
   * @param string $queueName
   * @param mixed $data
   * @param int $delay
   * @param int $priority
   * @param string|null $taskId
   * @return string|false The task ID on success, false on failure.
   * @throws Throwable
   */
  public static function sendTo(string $connectionName, string $queueName, mixed $data, int $delay = 0, int $priority = 0, ?string $taskId = null): string|false
  {
    return Redis::connection($connectionName)->send($queueName, $data, $delay, $priority, $taskId);
  }

  /**
   * Cancel a message from a sequency queue.
   * @param string $queueName The specific queue name (e.g., "emails", "notifications").
   * @param string $taskId The ID of the task to cancel.
   * @return bool
   * @throws Throwable
   */
  public static function cancel(string $queueName, string $taskId): bool
  {
    return Redis::connection()->cancel($queueName, $taskId);
  }

  /**
   * Cancel a message from a sequency queue on a specific connection.
   * @param string $connectionName
   * @param string $queueName
   * @param string $taskId
   * @return bool
   * @throws Throwable
   */
  public static function cancelFrom(string $connectionName, string $queueName, string $taskId): bool
  {
    return Redis::connection($connectionName)->cancel($queueName, $taskId);
  }
}