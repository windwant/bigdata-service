package org.windwant.bigdata.storm.redis;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

/**
 * Created by Administrator on 18-9-17.
 */
public class MyRedisMapper implements RedisStoreMapper {
    private RedisDataTypeDescription description;

    private final String hashKey = "wordCount";

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return description = new RedisDataTypeDescription(
                RedisDataTypeDescription.RedisDataType.HASH, hashKey);
    }

    @Override
    public String getKeyFromTuple(ITuple iTuple) {
        return iTuple.getStringByField("word");
    }

    @Override
    public String getValueFromTuple(ITuple iTuple) {
        return iTuple.getValueByField("count").toString();
    }
}
