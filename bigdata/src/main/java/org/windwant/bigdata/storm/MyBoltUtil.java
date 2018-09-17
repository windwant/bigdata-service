package org.windwant.bigdata.storm;

import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.windwant.bigdata.storm.mongodb.MyMongoMapper;
import org.windwant.bigdata.storm.redis.MyRedisMapper;

/**
 * Created by Administrator on 18-9-17.
 */
public class MyBoltUtil {

    static JedisPoolConfig poolConfig;

    private MyBoltUtil(){
    }

    public static RedisStoreBolt getRedisStoreBolt(){
        if(poolConfig == null) {
            poolConfig = new JedisPoolConfig.Builder()
                    .setHost("localhost")
                    .setPort(6379)
                    .build();
        }
        return new RedisStoreBolt(poolConfig, new MyRedisMapper());
    }

    public static MongoInsertBolt getMongoInsertBolt(){
        return new MongoInsertBolt("mongodb://localhost:27017/mdb", "wordcount",
                new MyMongoMapper().withFields("word", "count"));
    }
}
