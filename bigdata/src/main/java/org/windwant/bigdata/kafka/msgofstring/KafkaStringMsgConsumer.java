package org.windwant.bigdata.kafka.msgofstring;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka consumer
 */
public class KafkaStringMsgConsumer {
    public static final Logger logger = LoggerFactory.getLogger(KafkaStringMsgConsumer.class);

    private Properties props;
    public static void main(String[] args) throws ConfigurationException {
        new KafkaStringMsgConsumer().start();
    }

    public KafkaStringMsgConsumer() throws ConfigurationException {
        props = new Properties();
        PropertiesConfiguration config = new PropertiesConfiguration("kafka.properties");
        config.setReloadingStrategy(new FileChangedReloadingStrategy());
        //自动保存
        config.setAutoSave(true);
        props.put("value.deserializer", config.getString("value.string.deserializer"));
        props.put("key.deserializer", config.getString("key.deserializer"));
        props.put("bootstrap.servers", config.getString("bootstrap.servers"));
        props.put("group.id", config.getString("group.id"));
        props.put("zookeeper.connect", config.getString("zookeeper.connect"));
        props.put("value.deserializer.encoding", config.getString("value.deserializer.encoding"));
    }

    public void  start(){
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("partition_test")); //storm-topic streams-wordcount-output
        Map topics = consumer.listTopics();
        logger.info("topic list {}", topics);
        Map metrics = consumer.metrics();
        logger.info("metrics {}", metrics);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1);
            for (ConsumerRecord<String, String> record : records) {
                logger.info(record.toString());
            }
        }
    }
}
