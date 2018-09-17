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

import java.util.Map;

/**
 * 字符串分割=》
*/
public class SplitWordBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(SplitWordBolt.class);
    private static final long serialVersionUID = 886149895478467894L;
    private OutputCollector collector; //输出收集

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        logger.info("receive data: {}", input);
        String msg = input.getStringByField("value"); //获取消息内容
        String[] words = msg.split("\\s+");//空格分割
        for(String word : words) {
            logger.info("emit: {}", word);
            collector.emit(input, new Values(word, 1));
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}