package org.windwant.bigdata.kafka;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.windwant.common.Constants;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Kafka producer
 *
 */
public class MyKafkaProducer {
    public static final Logger logger = LoggerFactory.getLogger(MyKafkaProducer.class);

    private static final String[] cities = new String[]{""};

    private Properties props;
    public static void main(String[] args) throws ConfigurationException {
        new MyKafkaProducer().start();
    }

    public MyKafkaProducer() throws ConfigurationException {
        props = new Properties();
        PropertiesConfiguration config = new PropertiesConfiguration("kafka.properties");
        config.setReloadingStrategy(new FileChangedReloadingStrategy());
        config.setAutoSave(true);
        props.put("value.serializer", config.getString("value.serializer"));
        props.put("key.serializer", config.getString("key.serializer"));
        props.put("request.required.acks", config.getString("request.required.acks"));
        props.put("bootstrap.servers", config.getString("bootstrap.servers"));
    }

    public void start(){
        try {
            Producer<String, String> producer = new KafkaProducer<>(props);
            for(int i = 0; i < Integer.MAX_VALUE; i++) {
                ProducerRecord record = new ProducerRecord<>("storm-topic", "msg", Constants.getRandomCity());
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
