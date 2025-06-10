<?php

namespace Webman\Sequency;

class Install
{
  const bool WEBMAN_PLUGIN = true;

  /**
   * Install
   * @return void
   */
  public static function install(): void
  {
    // 创建消费者类存放的目录
    $consumer_dir = app_path() . '/queue/sequency';
    if (!is_dir($consumer_dir)) {
      mkdir($consumer_dir, 0777, true);
    }
  }

  /**
   * Uninstall
   * @return void
   */
  public static function uninstall(): void
  {
  }
}