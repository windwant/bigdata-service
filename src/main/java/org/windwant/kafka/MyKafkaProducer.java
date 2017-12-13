package org.windwant.kafka;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.windwant.Constants;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by aayongche on 2016/9/12.
 */
public class MyKafkaProducer {

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
                RecordMetadata result = producer.send(new ProducerRecord<>("storm-topic",
                        "msg", Constants.getRandomCity())).get();
                System.out.println("producer send: " + result);
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
