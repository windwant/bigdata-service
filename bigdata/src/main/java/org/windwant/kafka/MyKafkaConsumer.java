package org.windwant.kafka;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

/**
 * Kafka consumer
 */
public class MyKafkaConsumer {
    public static final Logger logger = LoggerFactory.getLogger(MyKafkaConsumer.class);

    private Properties props;
    public static void main(String[] args) throws ConfigurationException {
        new MyKafkaConsumer().start();
    }

    public MyKafkaConsumer() throws ConfigurationException {
        props = new Properties();
        PropertiesConfiguration config = new PropertiesConfiguration("kafka.properties");
        config.setReloadingStrategy(new FileChangedReloadingStrategy());
        //自动保存
        config.setAutoSave(true);
        props.put("value.deserializer", config.getString("value.deserializer"));
        props.put("key.deserializer", config.getString("key.deserializer"));
        props.put("bootstrap.servers", config.getString("bootstrap.servers"));
        props.put("group.id", config.getString("group.id"));
    }

    public void  start(){
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("storm-topic"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                logger.info(record.toString());
            }
        }
    }
}
