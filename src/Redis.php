<?php

namespace Webman\Sequency;

use RedisException;
use RuntimeException;
use Webman\Context;
use Workerman\Coroutine\Pool;
use Throwable;

class Redis
{
  protected static array $pools = [];

  /**
   * @param string $name
   * @return RedisConnection
   * @throws RuntimeException
   */
  public static function connection(string $name = 'default'): RedisConnection
  {
    $name = $name ?: 'default';
    $key = "sequency.connections.$name";
    $connection = Context::get($key);
    if (!$connection) {
      if (!isset(static::$pools[$name])) {
        $configs = config('sequency', config('plugin.webman.sequency.redis', []));
        if (!isset($configs[$name])) {
          throw new RuntimeException("Sequency Redis connection $name not found");
        }
        $config = $configs[$name];
        $poolConfig = $config['pool'] ?? [
          'max_connections' => 10,
          'min_connections' => 1,
          'wait_timeout' => 3,
          'idle_timeout' => 60,
          'heartbeat_interval' => 55,
        ];
        $pool = new Pool($poolConfig['max_connections'] ?? 10);
        $pool->setConnectionCreator(function () use ($config) {
          return static::connect($config); // Pass full $config here
        });
        $pool->setConnectionCloser(function ($connection) {
          if ($connection instanceof RedisConnection) {
            $connection->close();
          }
        });
        $pool->setHeartbeatChecker(function ($connection) {
          if ($connection instanceof RedisConnection) {
            try {
              return $connection->ping() === '+PONG';
            } catch (Throwable) {
              return false;
            }
          }
          return false;
        });
        static::$pools[$name] = $pool;
      }

      $connection = static::$pools[$name]->get();
      Context::set($key, $connection);

      Context::onDestroy(function () use ($connection, $name) {
        if ($connection && isset(static::$pools[$name])) {
          static::$pools[$name]->put($connection);
        }
      });
    }
    return $connection;
  }

  /**
   * @param array $config The specific connection config array from redis.php
   * @return RedisConnection
   * @throws RedisException
   */
  protected static function connect(array $config): RedisConnection
  {
    if (!extension_loaded('redis')) {
      throw new RuntimeException('Please make sure the PHP Redis extension is installed and enabled for Sequency.');
    }
    $redis = new RedisConnection;
    // Prepare config for RedisConnection's connectWithConfig
    $connectionParams = [
      'host' => $config['host'] ?? 'redis://127.0.0.1:6379', // Default host string
      'options' => $config['options'] ?? [],
    ];

    $addressParts = parse_url($connectionParams['host']);
    $finalConfig = [
      'host' => $addressParts['host'] ?? '127.0.0.1',
      'port' => $addressParts['port'] ?? 6379,
      'auth' => $connectionParams['options']['auth'] ?? null,
      'db' => $connectionParams['options']['db'] ?? 0,
      'prefix' => $connectionParams['options']['prefix'] ?? '',
      'timeout' => $connectionParams['options']['timeout'] ?? 2.0,
    ];
    if (str_starts_with($connectionParams['host'], 'unix:/')) {
      $finalConfig['host'] = $connectionParams['host'];
      $finalConfig['port'] = null; // Port is not used for unix sockets
    }
    $redis->connectWithConfig($finalConfig);
    return $redis;
  }

  public static function __callStatic($name, $arguments)
  {
    return static::connection()->{$name}(...$arguments);
  }
}