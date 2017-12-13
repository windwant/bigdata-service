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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

public class MyKafkaStorm {

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
            logger.info("receive data: {}", line);
            String[] words = line.split("\\s+");
            for(String word : words) {
                logger.info("emit: {}", word);
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
    public static class CountWordBold extends BaseRichBolt {

        private static final Logger logger = LoggerFactory.getLogger(CountWordBold.class);
        private static final long serialVersionUID = 886148754599637894L;
        private OutputCollector collector;
        private Map<String, AtomicInteger> statistic;

        @Override
        public void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
            this.statistic = new HashMap<>();//计数Map
        }

        @Override
        public void execute(Tuple input) {
            String word = input.getString(0);
            int count = input.getInteger(1);
            logger.info("receive data，word: {}, count: {}", word, count);
            AtomicInteger wordCounter = this.statistic.get(word);
            if(wordCounter == null) {
                wordCounter = new AtomicInteger();
                this.statistic.put(word, wordCounter);
            }
            wordCounter.addAndGet(count);
            collector.ack(input);
            logger.info("word count map: {}", this.statistic);
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
        builder.setBolt("word-counter", new CountWordBold()).fieldsGrouping("word-splitter", new Fields("word"));

        Config conf = new Config();

        String name = MyKafkaStorm.class.getSimpleName();
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