package org.windwant.bigdata.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * kafka消息生成方式：
 *   1. LogGenerator生成测试日志发送到flume=》
 *   2. MyKafkaProducer发送测试消息
 *   3. elasticjob定时任务模块生成测试日志
 *
 * MyKafkaStormHdfs 实时处理消息：
 *   =》读取kafka数据
 *   =》storm实时处理（分割字符，统计字符）
 *   =》写入hdfs
 *
 * 注意各依赖的版本问题
 *
 * kafka 启动：bin/kafka-server-start config/server.properties
 * hadoop 启动：bin/start-dfs bin/start-yarn
 */
public class MyKafkaStormHdfs {

    private static final String bootstrapServers = "localhost:9092"; //broker host 避免写成zookeeper地址
    private static final String testTopic = "partition_test"; //测试主题

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, AuthorizationException {

        KafkaSpoutConfig.Builder kbuilder = KafkaSpoutConfig.builder(bootstrapServers, testTopic);

        //TopologyBuilder是构建拓扑的类，用于指定执行的拓扑。拓扑底层是Thrift结构，由于Thrift API非常冗长，使用TopologyBuilder可以极大地简化建立拓扑的过程
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(kbuilder.build()), 5);//id为kafka-reader 并行度为5
        //builder.setSpout("kafka-reader", new RandomSentenceSpout(), 3);
        //builder.setBolt("printBolt",new PrintBolt(),2).localOrShuffleGrouping("kafka-reader");
        builder.setBolt("word-splitter", new SplitWordBolt(), 2).shuffleGrouping("kafka-reader");
        builder.setBolt("word-counter", new CountWordBolt()).fieldsGrouping("word-splitter", new Fields("word"));
        builder.setBolt("hdfs-bolt", new ToHdfsBolt(), 2).shuffleGrouping("word-counter");

        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);

        String name = MyKafkaStormHdfs.class.getSimpleName();
        if (args != null && args.length > 0) {
            conf.put(Config.NIMBUS_HOST, args[0]);
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, conf, builder.createTopology());
            Thread.sleep(1000000);
            cluster.shutdown();
        }
    }
}