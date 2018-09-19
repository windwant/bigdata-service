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

    public static int add(int... args){
        int sum=0;
        for(int i=0;i<args.length;i++){
            sum+=args[i];
        }
        return sum;
    }

    public static void main(String[] args) {
        System.out.println(add(1, 2,4));
    }
}
