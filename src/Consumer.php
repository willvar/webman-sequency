<?php

namespace Webman\Sequency;

use Throwable;

interface Consumer
{
  /**
   * Consumes the given data from the queue.
   * @param mixed $data The data dequeued.
   * @return void
   * @throws Throwable If an error occurs that should cause a retry or failure.
   * @throws NonRetryableException If an error occurs that should NOT be retried.
   */
  public function consume(mixed $data): void;
}