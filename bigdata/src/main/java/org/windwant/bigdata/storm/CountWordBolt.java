package org.windwant.bigdata.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
  * 计算频次
 **/
public class CountWordBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(CountWordBolt.class);
    private static final long serialVersionUID = 886148754599637894L;
    private OutputCollector collector;
    private Map<String, AtomicInteger> statistic;
    private Tuple tuple;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.statistic = new HashMap<>();//计数Map
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        this.tuple = input;
        int count = input.getInteger(1);
        logger.info("receive data, word: {}, count: {}", word, count);
        AtomicInteger wordCounter = this.statistic.get(word);
        if(wordCounter == null) {
            wordCounter = new AtomicInteger();
            this.statistic.put(word, wordCounter);
        }
        wordCounter.addAndGet(count);
        collector.ack(input);
        collector.emit(tuple, new Values(word, wordCounter));

        logger.info("word count map: {}", this.statistic);
    }

    @Override
    public void cleanup() {
        logger.info("final statistic result: ");
        Iterator<Map.Entry<String, AtomicInteger>> it = this.statistic.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry<String, AtomicInteger> entry = it.next();
            logger.info("word: {}, count: {}", entry.getKey(), entry.getValue().get());
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}