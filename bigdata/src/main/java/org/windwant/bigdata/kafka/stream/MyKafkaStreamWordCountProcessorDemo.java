package org.windwant.bigdata.kafka.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates, using the low-level Processor APIs, how to implement the WordCount program
 * that computes a simple word occurrence histogram from an input text.
 *
 * In this example, the input stream reads from a topic named "streams-plaintext-input", where the values of messages
 * represent lines of text; and the histogram output is written to topic "streams-wordcount-processor-output" where each record
 * is an updated count of a single word.
 *
 * Before running this example you must create the input topic and the output topic (e.g. via
 * bin/kafka-topics.sh --create ...), and write some data to the input topic (e.g. via
 * bin/kafka-console-producer.sh). Otherwise you won't see any data arriving in the output topic.
 */
public class MyKafkaStreamWordCountProcessorDemo {

    //用于生成Topology使用的Processor
    static class MyProcessorSupplier implements ProcessorSupplier<String, String> {

        @Override
        public Processor<String, String> get() {
            return new Processor<String, String>() {
                //Process上下文环境 元数据（数据记录，kafka topic partition message offset
                private ProcessorContext context;
                private KeyValueStore<String, Integer> kvStore;

                @Override
                @SuppressWarnings("unchecked")
                public void init(final ProcessorContext context) {
                    this.context = context;
                    //Processor定时任务，每隔1s stream时间（record时间），WALL_CLOCK_TIME（系统时间）
                    this.context.schedule(1000, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                        try (KeyValueIterator<String, Integer> iter = kvStore.all()) {
                            //输出间隔统计结果 间隔内消息汇总
                            System.out.println("----------- " + timestamp + " ----------- ");
                            while (iter.hasNext()) {
                                KeyValue<String, Integer> entry = iter.next();
                                System.out.println("[" + entry.key + ", " + entry.value + "]");
                                context.forward(entry.key, entry.value.toString());
                            }
                        }
                    });

                    this.kvStore = (KeyValueStore<String, Integer>) context.getStateStore("Counts");
                }

                @Override
                public void process(String dummy, String line) {
                    //topic 消息类型 I want sth, 分割消息为数组
                    String[] words = line.toLowerCase(Locale.getDefault()).split(" ");
                    for (String word : words) {
                        Integer oldValue = this.kvStore.get(word);
                        if (oldValue == null) {
                            this.kvStore.put(word, 1);
                        } else {
                            this.kvStore.put(word, oldValue + 1);
                        }
                    }

                    //提交当前处理进程
                    context.commit();
                }

                @Override
                public void close() {
                }
            };
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); //kafka broker host
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); //key 序列化
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass()); //value 序列化

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //A topology 是包含sources, processors和sinks的无回路有向图
        //sources: 消费kafka主题消息，并发送到后续节点
        //processors: 接收上一个节点输入记录，处理记录，转发至下游节点
        //sinks: 接受上一点节点记录，输出到kafka主题
        //用于构造KStream进行下一步消息处理
        Topology builder = new Topology();
        //消费kafka主题 partition_test
        builder.addSource("Source", "partition_test")
            //添加Processor，名称 Process，上一节点 Source
            .addProcessor("Process", new MyProcessorSupplier(), "Source")
            //添加状态存储，用于 Processor 使用，store名称为Counts，可以使用store的Processor为Process
            .addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("Counts"),//内存存储
                Serdes.String(),
                Serdes.Integer()),
                "Process")
            //添加输出，主题 上一节点 Process
            .addSink("Sink", "streams-wordcount-output", "Process");

        //构造KafkaStream
        final KafkaStreams streams = new KafkaStreams(builder, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}