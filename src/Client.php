<?php
namespace Webman\Sequency;

use Throwable;

class Client
{
  /**
   * @param string $queueName
   * @param mixed $data
   * @param int $delay
   * @param int|null $priority
   * @return bool
   * @throws Throwable
   */
  public static function send(string $queueName, mixed $data, int $delay = 0, ?int $priority = null): bool
  {
    return Redis::connection()->send($queueName, $data, $delay, $priority);
  }

  /**
   * Send to a specific connection.
   * @param string $connectionName
   * @param string $queueName
   * @param mixed $data
   * @param int $delay
   * @param int|null $priority Priority of the job. Uses default if null.
   * @return bool
   * @throws Throwable
   */
  public static function sendTo(string $connectionName, string $queueName, mixed $data, int $delay = 0, ?int $priority = null): bool
  {
    return Redis::connection($connectionName)->send($queueName, $data, $delay, $priority);
  }
}