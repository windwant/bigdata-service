package org.windwant.bigdata.kafka.msgofentity;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.windwant.bigdata.kafka.model.Payload;
import org.windwant.common.Constants;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Kafka producer
 */
public class KafkaEntityMsgProducer {
    public static final Logger logger = LoggerFactory.getLogger(KafkaEntityMsgProducer.class);

    private static final String[] cities = new String[]{""};

    private Properties props;
    public static void main(String[] args) throws ConfigurationException {
        new KafkaEntityMsgProducer().start();
    }

    public KafkaEntityMsgProducer() throws ConfigurationException {
        props = new Properties();
        PropertiesConfiguration config = new PropertiesConfiguration("kafka.properties");
        config.setReloadingStrategy(new FileChangedReloadingStrategy());
        config.setAutoSave(true);
        props.put("value.serializer", config.getString("value.entity.serializer"));
        props.put("value.type.class", Payload.class);
        props.put("key.serializer", config.getString("key.serializer"));
        props.put("request.required.acks", config.getString("request.required.acks"));
        props.put("bootstrap.servers", config.getString("bootstrap.servers"));
        props.put("partitioner.class", config.getString("partitioner.class"));
        props.put("value.serializer.encoding", config.getString("value.serializer.encoding"));
    }

    public void start(){
        try {
            Producer<String, String> producer = new KafkaProducer<>(props);
            for(int i = 0; i < Integer.MAX_VALUE; i++) { // //storm-topic
                Payload payload = new Payload();
                payload.setStatus(i);
                payload.setCause(Constants.getRandomCity(5));
                ProducerRecord record = new ProducerRecord<>("payload_t", "msg", payload);
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
