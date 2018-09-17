package org.windwant.bigdata.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class PrintBolt extends BaseBasicBolt {
        private static final Logger logger = LoggerFactory.getLogger(PrintBolt.class);

        /**
         * @param tuple
         * @param basicOutputCollector
         */
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            logger.info(Arrays.toString(tuple.getValues().toArray()));
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }