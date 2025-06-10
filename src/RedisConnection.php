<?php

namespace Webman\Sequency;

use RuntimeException;
use Throwable;

class RedisConnection extends \Redis
{
  protected array $config = [];
  // Define queue names for Sequency
  const string QUEUE_WAITING_PREFIX = '{sequency}-waiting:'; // Prefix for sorted sets (per queue)
  const string QUEUE_DELAYED_KEY = '{sequency}-delayed';     // Single key for all delayed jobs (Sorted Set)
  const string QUEUE_FAILED_KEY = '{sequency}-failed';       // Single key for all failed jobs (List)
  const string QUEUE_TASK_INDEX_KEY = '{sequency}-tasks';      // Hash to map taskId to its package string for fast cancellation.
  const int MAX_INPUT_PRIORITY = 999; // The highest number the user will pass for priority, any value larger than this will regard same
  const int MIN_INPUT_PRIORITY = 0;

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
   * @param string|null $taskId
   * @return string|false The task ID on success, false on failure.
   * @throws Throwable
   */
  public function send(string $queueName, mixed $data, int $delay = 0, ?int $priority = null, ?string $taskId = null): string|false
  {
    $priority = $priority ?? 0;
    $now = time();
    $taskId = $taskId ?? uuid_create();
    $package = [
      'id' => $taskId,
      'time' => $now,
      'delay' => $delay,
      'attempts' => 0,
      'queue' => $queueName,
      'priority' => $priority,
      'data' => $data
    ];
    $packageStr = igbinary_serialize($package);

    // Atomically add to the queue and the task index
    $this->multi();
    $this->hSet(self::QUEUE_TASK_INDEX_KEY, $taskId, $packageStr);

    if ($delay > 0) {
      $this->zAdd(self::QUEUE_DELAYED_KEY, $now + $delay, $packageStr);
    } else {
      $score = self::calculatePriorityScore($priority, $now);
      $this->zAdd(self::QUEUE_WAITING_PREFIX . $queueName, $score, $packageStr);
    }

    $result = $this->exec();

    // Check if all commands in the transaction succeeded
    if ($result && !in_array(false, $result, true)) {
      return $taskId;
    }

    // If something failed, attempt to clean up the index entry
    $this->hDel(self::QUEUE_TASK_INDEX_KEY, $taskId);
    return false;
  }

  /**
   * Cancel a message from a sequency queue. (Optimized version)
   * @param string $queueName The specific queue name. This is kept for API consistency but not strictly needed for the lookup.
   * @param string $taskId The ID of the task to cancel.
   * @return bool
   * @throws Throwable
   */
  public function cancel(string $queueName, string $taskId): bool
  {
    // Look up the task package by its ID from the hash index.
    $packageStr = $this->execCommand('hGet', self::QUEUE_TASK_INDEX_KEY, $taskId);
    if (empty($packageStr)) {
      // Task does not exist or has already been processed.
      return false;
    }
    $task = igbinary_unserialize($packageStr);
    if (!$task || !isset($task['delay'])) {
      // Invalid package, try to clean up the index.
      $this->execCommand('hDel', self::QUEUE_TASK_INDEX_KEY, $taskId);
      return false;
    }
    // Determine which queue the task is in.
    $isDelayed = ($task['delay'] > 0 && $task['attempts'] === 0) || ($task['attempts'] > 0);
    $queueKey = $isDelayed ? self::QUEUE_DELAYED_KEY : self::QUEUE_WAITING_PREFIX . $task['queue'];
    // Atomically remove the task from the correct queue and from the index hash.
    $this->multi();
    $this->zRem($queueKey, $packageStr);
    $this->hDel(self::QUEUE_TASK_INDEX_KEY, $taskId);
    $result = $this->exec();

    // The zRem result is at index 0. It returns the number of elements removed (0 or 1).
    return isset($result[0]) && $result[0] > 0;
  }
}