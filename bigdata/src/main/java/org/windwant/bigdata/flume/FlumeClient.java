package org.windwant.bigdata.flume;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Administrator on 18-3-21.
 */
public class FlumeClient {
    private static RpcClient flumeRpcClient = RpcClientFactory.getDefaultInstance("localhost", 4444);

    public static void main(String[] args) {
        int i = 0;
        while (i < 100){
            String log = "log message: info" + ThreadLocalRandom.current().nextInt(100);
            send(EventBuilder.withBody(log.getBytes()));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        flumeRpcClient.close();

    }

    public static void send(Event event){
        try {
            flumeRpcClient.append(event);
        } catch (EventDeliveryException e) {
            e.printStackTrace();
        }
    }

    public static void sendBatch(List<Event> list){
        try {
            flumeRpcClient.appendBatch(list);
        } catch (EventDeliveryException e) {
            e.printStackTrace();
        }
    }
}
