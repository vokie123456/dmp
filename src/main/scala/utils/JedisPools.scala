package utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object JedisPools {

    private val jedisPool = new JedisPool(new GenericObjectPoolConfig(), "mini6", 6379)

    def getJedis() = jedisPool.getResource

}