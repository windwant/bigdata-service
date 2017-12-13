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

public class MyKafkaTopology {

    /**
     * 字符串分割=》
     */
    public static class SplitWordBolt extends BaseRichBolt {

        private static final Logger logger = LoggerFactory.getLogger(SplitWordBolt.class);
        private static final long serialVersionUID = 886149197481637894L;
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
     * cal word
     */
    public static class CountWordBold extends BaseRichBolt {

        private static final Logger logger = LoggerFactory.getLogger(CountWordBold.class);
        private static final long serialVersionUID = 886149197481637894L;
        private OutputCollector collector;
        private Map<String, AtomicInteger> counterMap;

        @Override
        public void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
            this.counterMap = new HashMap<>();//计数Map
        }

        @Override
        public void execute(Tuple input) {
            String word = input.getString(0);
            int count = input.getInteger(1);
            logger.info("receive data，word: {}, count: {}", word, count);
            AtomicInteger wordCounter = this.counterMap.get(word);
            if(wordCounter == null) {
                wordCounter = new AtomicInteger();
                this.counterMap.put(word, wordCounter);
            }
            wordCounter.addAndGet(count);
            collector.ack(input);
            logger.info("word count map: {}", this.counterMap);
        }

        @Override
        public void cleanup() {
            logger.info("final statistic result: ");
            Iterator<Entry<String, AtomicInteger>> it = this.counterMap.entrySet().iterator();
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

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
        String zks = "localhost:2181/kafka";
        String topic = "storm-topic";
        String zkRoot = "/kafka"; // storm zookeeper root
        String id = "word";

        BrokerHosts brokerHosts = new ZkHosts(zks, "/brokers");
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.forceFromStart = false;
        spoutConf.zkServers = Arrays.asList(new String[] {"localhost"});
        spoutConf.zkPort = 2181;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 5);
        builder.setBolt("word-splitter", new SplitWordBolt(), 2).shuffleGrouping("kafka-reader");
        builder.setBolt("word-counter", new CountWordBold()).fieldsGrouping("word-splitter", new Fields("word"));

        Config conf = new Config();

        String name = MyKafkaTopology.class.getSimpleName();
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