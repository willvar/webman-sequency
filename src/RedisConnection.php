<?php

namespace Webman\Sequency;

use RuntimeException;
use Throwable;

class RedisConnection extends \Redis
{
  protected array $config = [];
  // Define queue names for Sequency
  const QUEUE_WAITING_PREFIX = '{sequency}-waiting:'; // Prefix for sorted sets (per queue)
  const QUEUE_DELAYED_KEY = '{sequency}-delayed';     // Single key for all delayed jobs (Sorted Set)
  const QUEUE_FAILED_KEY = '{sequency}-failed';       // Single key for all failed jobs (List)
  const MAX_INPUT_PRIORITY = 999; // The highest number the user will pass for priority, any value larger than this will regard same
  const MIN_INPUT_PRIORITY = 0;

  public function connectWithConfig(array $config = []): void
  {
    if ($config) {
      $this->config = $config;
    }
    $host = $this->config['host'] ?? '127.0.0.1';
    $port = $this->config['port'] ?? 6379;
    $timeout = $this->config['timeout'] ?? 2.0; // float for timeout

    if (str_starts_with($host, 'unix:/')) {
      if (false === $this->connect($host)) { // port is ignored for unix sockets
        throw new RuntimeException("Redis connect to $host fail.");
      }
    } elseif (false === $this->connect($host, (int)$port, (float)$timeout)) {
      throw new RuntimeException("Redis connect $host:$port fail.");
    }

    if (!empty($this->config['auth'])) {
      $this->auth($this->config['auth']);
    }
    if (!empty($this->config['db'])) {
      $this->select((int)$this->config['db']);
    }
    if (!empty($this->config['prefix'])) {
      $this->setOption(\Redis::OPT_PREFIX, $this->config['prefix']);
    }
  }

  /**
   * @throws Throwable
   */
  public function execCommand($command, ...$args)
  {
    try {
      return $this->{$command}(...$args);
    } catch (Throwable $e) {
      $msg = strtolower($e->getMessage());
      if (str_contains($msg, 'connection lost') || str_contains($msg, 'went away') || str_contains($msg, 'read error on connection')) {
        $this->close(); // Close previous connection first
        $this->connectWithConfig(); // Reconnect
        return $this->{$command}(...$args);
      }
      throw $e;
    }
  }

  /**
   * Calculates a lexicographically sortable score for a job.
   * Lower scores have higher precedence (higher priority, then older timestamp).
   * @param int $userPriority The user-defined priority.
   * @param int $timestamp The job's enqueue timestamp.
   * @return string The calculated score.
   */
  public static function calculatePriorityScore(int $userPriority, int $timestamp): string
  {
    // Normalize and cap userPriority to the defined input range
    $normalizedUserPriority = max(self::MIN_INPUT_PRIORITY, min(self::MAX_INPUT_PRIORITY, $userPriority));
    // Score part: Higher user input priority should result in a smaller score component.
    // Example: P9 (MAX_INPUT_PRIORITY) -> (9 - 9) = 0 (smallest score part for highest priority).
    //          P0 (MIN_INPUT_PRIORITY) -> (9 - 0) = 9 (largest score part for lowest priority).
    // Using %03d for priority part allows for up to 1000 distinct priority levels
    // if MAX_INPUT_PRIORITY were, for example, 999.
    // For 0-9, this will result in "000", "001", ..., "009" for the priority score part.
    return sprintf('%0' . strlen(self::MAX_INPUT_PRIORITY) . 'd%010d', self::MAX_INPUT_PRIORITY - $normalizedUserPriority, $timestamp);
  }

  /**
   * Send a message to a sequency queue.
   * @param string $queueName The specific queue name (e.g., "emails", "notifications").
   * @param mixed $data The data to be queued.
   * @param int $delay Delay in seconds.
   * @param int|null $priority The priority of the job (e.g., 1-10, higher number means higher priority).
   * @return bool
   * @throws Throwable
   */
  public function send(string $queueName, mixed $data, int $delay = 0, ?int $priority = null): bool
  {
    $priority = $priority ?? 0;
    $now = time(); // Original enqueue time
    $packageStr = json_encode_lite([
      'id' => uuid_create(),
      'time' => $now, // Store the original enqueue time
      'delay' => $delay,
      'attempts' => 0,
      'queue' => $queueName,
      'priority' => $priority, // Store the job's priority
      'data' => $data
    ]);
    if ($delay > 0) {
      // All delayed jobs go into one sorted set, scored by their execution time.
      // The package contains the target queue and priority.
      return (bool)$this->execCommand('zAdd', self::QUEUE_DELAYED_KEY, $now + $delay, $packageStr);
    }
    // Immediate jobs go to their specific queue's sorted set, scored by priority and time.
    $score = self::calculatePriorityScore($priority, $now);
    return (bool)$this->execCommand('zAdd', self::QUEUE_WAITING_PREFIX . $queueName, $score, $packageStr);
  }
}