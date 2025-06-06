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
   * @return bool
   * @throws Throwable
   */
  public static function send(string $queueName, mixed $data, int $delay = 0, int $priority = 0): bool
  {
    return Redis::connection()->send($queueName, $data, $delay, $priority);
  }

  /**
   * Send to a specific connection.
   * @param string $connectionName
   * @param string $queueName
   * @param mixed $data
   * @param int $delay
   * @param int $priority
   * @return bool
   * @throws Throwable
   */
  public static function sendTo(string $connectionName, string $queueName, mixed $data, int $delay = 0, int $priority = 0): bool
  {
    return Redis::connection($connectionName)->send($queueName, $data, $delay, $priority);
  }
}