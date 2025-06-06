<?php

namespace Webman\Sequency;

/**
 * A shared utility class for constants and static helper methods
 * used by both Producer (Client) and Consumer.
 */
class RedisConnection
{
  // Define queue names for Sequency
  const QUEUE_WAITING_PREFIX = '{sequency}-waiting:';
  const QUEUE_DELAYED_KEY = '{sequency}-delayed';
  const QUEUE_FAILED_KEY = '{sequency}-failed';
  const MAX_INPUT_PRIORITY = 999;
  const MIN_INPUT_PRIORITY = 0;

  /**
   * Calculates a lexicographically sortable score for a job.
   * Lower scores have higher precedence (higher priority, then older timestamp).
   * @param int $userPriority The user-defined priority.
   * @param int $timestamp The job's enqueue timestamp.
   * @return string The calculated score.
   */
  public static function calculatePriorityScore(int $userPriority, int $timestamp): string
  {
    $normalizedUserPriority = max(self::MIN_INPUT_PRIORITY, min(self::MAX_INPUT_PRIORITY, $userPriority));
    $priorityScorePart = self::MAX_INPUT_PRIORITY - $normalizedUserPriority;

    return sprintf(
      '%0' . strlen((string)self::MAX_INPUT_PRIORITY) . 'd%010d',
      $priorityScorePart,
      $timestamp
    );
  }
}