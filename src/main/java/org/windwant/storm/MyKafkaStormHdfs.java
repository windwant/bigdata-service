package org.windwant.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * kafka消息生成方式：
 *   1. LogGenerator生成测试日志发送到flume=》
 *   2. MyKafkaProducer发送测试消息
 *
 * MyKafkaStormHdfs 实时处理消息：
 *   =》读取kafka数据
 *   =》storm实时处理（分割字符，统计字符）
 *   =》写入hdfs
 *
 * 注意各依赖的版本问题
 */
public class MyKafkaStormHdfs {

    /**
     * 字符串分割=》
     */
    public static class SplitWordBolt extends BaseRichBolt {
        private static final Logger logger = LoggerFactory.getLogger(SplitWordBolt.class);
        private static final long serialVersionUID = 886149895478467894L;
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            String line = input.getString(0);
//            logger.info("receive data: {}", line);
            String[] words = line.split("\\s+");
            for(String word : words) {
//                logger.info("emit: {}", word);
                collector.emit(input, new Values(word, 1));
            }
            collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

    }

    /**
     * 词算
     */
    public static class CountWordBolt extends BaseRichBolt {

        private static final Logger logger = LoggerFactory.getLogger(CountWordBolt.class);
        private static final long serialVersionUID = 886148754599637894L;
        private OutputCollector collector;
        private Map<String, AtomicInteger> statistic;
        private Tuple tuple;

        @Override
        public void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
            this.statistic = new HashMap<>();//计数Map
        }

        @Override
        public void execute(Tuple input) {
            String word = input.getString(0);
            this.tuple = input;
            int count = input.getInteger(1);
//            logger.info("receive data，word: {}, count: {}", word, count);
            AtomicInteger wordCounter = this.statistic.get(word);
            if(wordCounter == null) {
                wordCounter = new AtomicInteger();
                this.statistic.put(word, wordCounter);
            }
            wordCounter.addAndGet(count);
            collector.ack(input);
            collector.emit(tuple, new Values(word, wordCounter));

//            logger.info("word count map: {}", this.statistic);
        }

        @Override
        public void cleanup() {
            logger.info("final statistic result: ");
            Iterator<Entry<String, AtomicInteger>> it = this.statistic.entrySet().iterator();
            while(it.hasNext()) {
                Entry<String, AtomicInteger> entry = it.next();
                logger.info("word: {}, count: {}", entry.getKey(), entry.getValue().get());
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    /**
     * 写hdfs
     */
    public static class ToHdfsBolt extends HdfsBolt{
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
        public void execute(Tuple tuple) {
//            super.execute(tuple);
            this.collector.ack(tuple);
            if(tupleMap.containsKey(tuple.getString(0))) {
                tupleMap.replace(tuple.getString(0), tuple);
            }else {
                tupleMap.put(tuple.getString(0), tuple);
            }

        }

        @Override
        public void cleanup() {
            //将最终的统计结果写入hdfs
            tupleMap.entrySet().stream().forEach(entry->{
                super.execute(entry.getValue());
                logger.info("hdfs write: {}", entry.getValue());
            });
        }
    }

    private static final String zkHost = "localhost:2181/kafka"; //zookeeper host
    private static final String testTopic = "storm-topic"; //测试主题
    private static final String zkRoot = "/kafka"; //zookeeper 根节点
    private static final String ID = "word";

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {

        BrokerHosts brokerHosts = new ZkHosts(zkHost, "/brokers");
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, testTopic, zkRoot, ID);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.forceFromStart = false;

        //TopologyBuilder是构建拓扑的类，用于指定执行的拓扑。拓扑底层是Thrift结构，由于Thrift API非常冗长，使用TopologyBuilder可以极大地简化建立拓扑的过程
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 5);//id为kafka-reader 并行度为5
        builder.setBolt("word-splitter", new SplitWordBolt(), 2).shuffleGrouping("kafka-reader");
        builder.setBolt("word-counter", new CountWordBolt()).fieldsGrouping("word-splitter", new Fields("word"));
        builder.setBolt("hdfs-bolt", new ToHdfsBolt(), 2).shuffleGrouping("word-counter");

        Config conf = new Config();

        String name = MyKafkaStormHdfs.class.getSimpleName();
        if (args != null && args.length > 0) {
            conf.put(Config.NIMBUS_HOST, args[0]);
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, conf, builder.createTopology());
            Thread.sleep(10000);
            cluster.shutdown();
        }
    }
}