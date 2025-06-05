<?php
namespace Webman\Sequency;

use FilesystemIterator;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;

class Install
{
  const WEBMAN_PLUGIN = true;

  /**
   * @var array
   */
  protected static array $pathRelation = [
    'config/plugin/webman/sequency' => 'config/plugin/webman/sequency',
  ];

  /**
   * Install
   * @return void
   */
  public static function install(): void
  {
    static::installByRelation();
    $consumer_dir = app_path() . '/queue/sequency';
    if (!is_dir($consumer_dir)) {
      mkdir($consumer_dir, 0777, true);
      file_put_contents($consumer_dir . '/.gitignore', "*\n!.gitignore\n!readme.md\n");
      file_put_contents($consumer_dir . '/readme.md', "This directory is for Webman\\Sequency consumer classes.\n");
    }
    echo "Webman\\Sequency installed successfully. Please configure your queues and consumers.\n";
  }

  /**
   * Uninstall
   * @return void
   */
  public static function uninstall(): void
  {
    self::uninstallByRelation();
    // Optionally, you could ask the user if they want to remove app/queue/sequency
    // For safety, usually, don't remove app-specific code on uninstall.
    echo "Webman\\Sequency uninstalled. Config files removed. App consumers at app/queue/sequency are not removed.\n";
  }

  public static function installByRelation(): void
  {
    foreach (static::$pathRelation as $source => $dest) {
      $sourcePath = __DIR__ . "/$source"; // Assuming config is in src/config relative to Install.php
      if (!is_dir($sourcePath)) { // Check if source path is correct
        $sourcePath = __DIR__ . "/../$source"; // If Install.php is in src/ then config is ../src/config -> ../config
      }
      if ($pos = strrpos($dest, '/')) {
        $parent_dir = base_path() . '/' . substr($dest, 0, $pos);
        if (!is_dir($parent_dir)) {
          mkdir($parent_dir, 0777, true);
        }
      }
      // Use copy_dir for directories
      if (is_dir($sourcePath)) {
        self::copy_dir($sourcePath, base_path() . "/$dest");
      } else if (is_file($sourcePath)) { // For single files if any in future
        copy($sourcePath, base_path() . "/$dest");
      }
    }
  }

  public static function uninstallByRelation(): void
  {
    foreach (static::$pathRelation as $dest) {
      $path = base_path() . "/$dest";
      if (!is_dir($path) && !is_file($path)) {
        continue;
      }
      self::remove_dir($path);
    }
  }

  // Helper function to copy directory (from webman/console or similar)
  protected static function copy_dir($source, $dest): void
  {
    if (!is_dir($dest)) {
      mkdir($dest, 0777, true);
    }
    foreach (
      $iterator = new RecursiveIteratorIterator(
        new RecursiveDirectoryIterator($source, FilesystemIterator::SKIP_DOTS),
        RecursiveIteratorIterator::SELF_FIRST
      ) as $item
    ) {
      if ($item->isDir()) {
        $childDir = $dest . DIRECTORY_SEPARATOR . $iterator->getSubPathName();
        if (!is_dir($childDir)) {
          mkdir($childDir, 0777, true);
        }
      } else {
        copy($item, $dest . DIRECTORY_SEPARATOR . $iterator->getSubPathName());
      }
    }
  }

  // Helper function to remove directory (from webman/console or similar)
  protected static function remove_dir($path): void
  {
    if (is_file($path) || is_link($path)) {
      unlink($path);
      return;
    }
    $dir = opendir($path);
    while (($file = readdir($dir)) !== false) {
      if ($file === '.' || $file === '..') {
        continue;
      }
      $file_path = $path . DIRECTORY_SEPARATOR . $file;
      if (is_dir($file_path)) {
        self::remove_dir($file_path);
      } else {
        unlink($file_path);
      }
    }
    closedir($dir);
    rmdir($path);
  }
}