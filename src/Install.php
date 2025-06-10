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
    // 确保插件配置目录存在
    $config_dir = config_path() . '/plugin/webman/sequency';
    if (!is_dir($config_dir)) {
      mkdir($config_dir, 0777, true);
    }
    // 创建 app.php 配置文件
    $app_config_file = $config_dir . '/app.php';
    if (!is_file($app_config_file)) {
      $content = <<<'EOF'
<?php
return [
  'enable' => true
];
EOF;
      file_put_contents($app_config_file, $content);
    }
    // 创建 process.php 配置文件
    $process_config_file = $config_dir . '/process.php';
    if (!is_file($process_config_file)) {
      $content = <<<'EOF'
<?php
return [
  'consumer' => [
    'handler' => Webman\Sequency\Process\Consumer::class,
    'count' => 1, // 可以根据需要调整进程数
    'constructor' => [
      'consumer_dir' => app_path() . '/queue/sequency'
    ]
    // 'eventLoop' => Workerman\Events\Fiber::class // 如果项目使用 Fiber，可以取消此行注释
  ]
];
EOF;
      file_put_contents($process_config_file, $content);
    }
    // 创建 log.php 配置文件
    $log_config_file = $config_dir . '/log.php';
    if (!is_file($log_config_file)) {
      $content = <<<'EOF'
<?php
return [
  'default' => [
    'handlers' => [
      [
        'class' => Monolog\Handler\RotatingFileHandler::class,
        'constructor' => [
          runtime_path() . '/logs/sequency.log', // 将日志文件名修改得更明确
          365, //$maxFiles
          config('app.debug') ? Monolog\Level::Debug : Monolog\Level::Info // 将 Notice 修改为 Info，更常用
        ],
        'formatter' => [
          'class' => Monolog\Formatter\LineFormatter::class,
          'constructor' => [
            "[%datetime%] %channel%.%level_name%: %message% %context% %extra%\n", // 更详细的日志格式
            'Y-m-d H:i:s', // 日期格式
            true, // aLlowInlineLineBreaks
            true  // ignoreEmptyContextAndExtra
          ]
        ]
      ]
    ]
  ]
];
EOF;
      file_put_contents($log_config_file, $content);
    }
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
    // 移除插件创建的配置文件
    $config_dir = config_path() . '/plugin/webman/sequency';
    self::remove_file($config_dir . '/app.php');
    self::remove_file($config_dir . '/process.php');
    self::remove_file($config_dir . '/log.php');
  }

  /**
   * Helper function to remove a file if it exists.
   * @param string $path
   */
  private static function remove_file(string $path): void
  {
    if (is_file($path)) {
      unlink($path);
    }
  }
}