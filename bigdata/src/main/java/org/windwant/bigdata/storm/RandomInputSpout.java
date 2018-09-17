package org.windwant.bigdata.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * 模拟数据流输入
*/
public class RandomInputSpout extends BaseRichSpout {

    SpoutOutputCollector spoutOutputCollector;
    Random random;

    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        spoutOutputCollector = collector;
        random = new Random();
    }

    // 进行Tuple处理的主要方法
    public void nextTuple() {
        Utils.sleep(2000);
        String[] sentences = new String[]{
                "A", "B", "C", "D", "E"};
        String sentence = sentences[random.nextInt(sentences.length)];
        // 使用emit方法进行Tuple发布，参数用Values申明
        spoutOutputCollector.emit(new Values(sentence.trim().toLowerCase()));
    }

    public void ack(Object id) {
    }

    public void fail(Object id) {
    }

    // 声明字段
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

}