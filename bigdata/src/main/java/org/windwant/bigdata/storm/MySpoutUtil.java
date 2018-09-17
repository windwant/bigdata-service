package org.windwant.bigdata.storm;

import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;

/**
 * Created by Administrator on 18-9-17.
 */
public class MySpoutUtil {

    private MySpoutUtil(){
    }

    public static KafkaSpout getKafkaSpout(String bootstrapServers, String... topics){
        KafkaSpoutConfig.Builder kbuilder = KafkaSpoutConfig.builder(bootstrapServers, topics);
        return new KafkaSpout(kbuilder.build());
    }
}
