package org.windwant.bigdata.kafka.msgofstring;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.windwant.common.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Kafka producer
 * .\kafka-topics.bat --describe --zookeeper localhost:2181 --topic partition_test
 */
public class KafkaStringMsgProducer {
    public static final Logger logger = LoggerFactory.getLogger(KafkaStringMsgProducer.class);

    private static final String[] cities = new String[]{""};

    private Properties props;

    private String[] sources = {};
    public static void main(String[] args) throws ConfigurationException {
        new KafkaStringMsgProducer().start();
    }

    public KafkaStringMsgProducer() throws ConfigurationException {
        props = new Properties();
        PropertiesConfiguration config = new PropertiesConfiguration("kafka.properties");
        config.setReloadingStrategy(new FileChangedReloadingStrategy());
        config.setAutoSave(true);
        props.put("value.serializer", config.getString("value.string.serializer"));
        props.put("key.serializer", config.getString("key.serializer"));
        props.put("request.required.acks", config.getString("request.required.acks"));
        props.put("bootstrap.servers", config.getString("bootstrap.servers"));
        props.put("partitioner.class", config.getString("partitioner.class"));
        props.put("value.serializer.encoding", config.getString("value.serializer.encoding"));
        sources = config.getStringArray("msg.source");
    }

    public void start(){
        try {
            Producer<String, String> producer = new KafkaProducer<>(props);
            for(int i = 0; i < Integer.MAX_VALUE; i++) { // //storm-topic
                ProducerRecord record = new ProducerRecord<>("partition_test", "msg",
                        sources[ThreadLocalRandom.current().nextInt(sources.length)]);
                logger.info("producer send, result: {}, msg: {}", producer.send(record).get(), record);
                Thread.sleep(1000);
            }
            producer.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
