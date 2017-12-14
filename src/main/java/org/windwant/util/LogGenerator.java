package org.windwant.util;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.windwant.Constants;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Administrator on 2017/12/5.
 *
 * 测试日志生成 发送到flume
 *
 * flume windows 启动命令: .\flume-ng.cmd agent --conf ..\conf --conf-file ..\conf\flume-conf.properties --name a1
 *
 */
public class LogGenerator {
    public static final Logger logger = LoggerFactory.getLogger(LogGenerator.class);
    static {
        Properties properties = new Properties();
        properties.setProperty("log4j.rootLogger", "INFO,application,console,flume");
        properties.setProperty("log4j.appender.application", "org.apache.log4j.DailyRollingFileAppender");
        properties.setProperty("log4j.appender.application.File", "logs/test.log");
        properties.setProperty("log4j.appender.application.Append", "true");
        properties.setProperty("log4j.appender.application.DatePattern", "'.'yyyy-MM-dd");
        properties.setProperty("log4j.appender.application.layout", "org.apache.log4j.PatternLayout");
        properties.setProperty("log4j.appender.application.layout.ConversionPattern", "%d [%t] %-5p %C{6} (%F:%L) - %m%n");

        properties.setProperty("log4j.appender.console", "org.apache.log4j.ConsoleAppender");
        properties.setProperty("log4j.appender.console.Target", "System.out");
        properties.setProperty("log4j.appender.console.layout", "org.apache.log4j.PatternLayout");
        properties.setProperty("log4j.appender.console.layout.ConversionPattern", "%d [%t] %-5p %C{6} (%F:%L) - %m%n");

        properties.setProperty("log4j.appender.flume", "org.apache.flume.clients.log4jappender.Log4jAppender");
        properties.setProperty("log4j.appender.flume.hostname", "localhost");
        properties.setProperty("log4j.appender.flume.port", "4444");
        properties.setProperty("log4j.appender.flume.layout", "org.apache.log4j.PatternLayout");
        properties.setProperty("log4j.appender.flume.layout.ConversionPattern", "%d [%t] %-5p %C{6} (%F:%L) - %m%n");
        PropertyConfigurator.configure(properties);

    }

    public static void main(String[] args) {
        while (true){
            logger.info("msg: {}", Constants.getRandomCity());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
