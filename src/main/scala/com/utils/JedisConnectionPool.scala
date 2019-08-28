package com.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Jedis
  */
object JedisConnectionPool {

  val  config = new JedisPoolConfig()

  config.setMaxTotal(20)


  config.setMaxIdle(10)

  val pool = new JedisPool(config,"192.169.52.3",6379,10000)

  def getConnection():Jedis = {
    pool.getResource

  }
}
