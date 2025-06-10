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
    // 注册消费者进程
    $config_dir = config_path() . '/plugin/webman/sequency';
    if (!is_dir($config_dir)) {
      mkdir($config_dir, 0777, true);
    }
    $process_config_file = $config_dir . '/process.php';
    if (!is_file($process_config_file)) {
      $content = <<<'EOF'
<?php

use Webman\Sequency\Process\Consumer;

return [
    'consumer' => [
        'handler' => Consumer::class,
        'constructor' => [
            // 消费者类所在的目录
            'consumer_dir' => app_path() . '/queue/sequency'
        ]
    ]
];
EOF;
      file_put_contents($process_config_file, $content);
    }
  }

  /**
   * Uninstall
   * @return void
   */
  public static function uninstall(): void
  {
    // 移除插件的进程配置文件
    $process_config_file = config_path() . '/plugin/webman/sequency/process.php';
    if (is_file($process_config_file)) {
      unlink($process_config_file);
    }
  }
}