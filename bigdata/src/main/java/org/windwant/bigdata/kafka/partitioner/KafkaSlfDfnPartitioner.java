package org.windwant.bigdata.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 选择发送分区
 * Created by Administrator on 18-7-31.
 */
public class KafkaSlfDfnPartitioner implements Partitioner {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSlfDfnPartitioner.class);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List partitions = cluster.partitionsForTopic(topic);
        int partition = ThreadLocalRandom.current().nextInt(partitions.size());
        logger.info("msg: {} sent to topic {} partition {}", value, topic, partition);
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
