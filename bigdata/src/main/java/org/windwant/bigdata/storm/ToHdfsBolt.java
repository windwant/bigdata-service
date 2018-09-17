package org.windwant.bigdata.storm;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 写hdfs
*/
public class ToHdfsBolt extends HdfsBolt {
    private static final Logger logger = LoggerFactory.getLogger(ToHdfsBolt.class);
    private OutputCollector collector;
    private Map<String, Tuple> tupleMap = new HashMap<>();

    public ToHdfsBolt() {
        super();
        this.withFsUrl("hdfs://localhost:9000") //hdfs地址
                .withFileNameFormat(new DefaultFileNameFormat() .withPath("/storm/").withPrefix("app_").withExtension(".log")) //文件名称
                .withRecordFormat(new DelimitedRecordFormat().withFieldDelimiter("\t")) //写入内容分割符
                .withRotationPolicy(new TimedRotationPolicy(1.0f, TimedRotationPolicy.TimeUnit.MINUTES)) // 按时间间隔滚动生成新文件
                .withSyncPolicy(new CountSyncPolicy(1000)); //sync the filesystem after every 1k tuples
    }

    @Override
    public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        super.doPrepare(conf, topologyContext, collector);
        this.collector = collector;
    }

    @Override
    public void cleanup() {
        //将最终的统计结果写入hdfs
        tupleMap.entrySet().stream().forEach(entry -> {
            super.execute(entry.getValue());
            logger.info("hdfs write: {}", entry.getValue());
        });
    }
}