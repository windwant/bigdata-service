package org.windwant.elasticjob.job;

import com.dangdang.ddframe.job.api.ShardingContext;
import com.dangdang.ddframe.job.api.simple.SimpleJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.windwant.common.Constants;


/**
 * Created by windwant on 2017/5/25.
 */
public class MySimpleJob implements SimpleJob {

    private static final Logger logger = LoggerFactory.getLogger(MySimpleJob.class);
    public void execute(ShardingContext shardingContext) {
        logger.info("msg: {}", Constants.getRandomCity());
    }
}
