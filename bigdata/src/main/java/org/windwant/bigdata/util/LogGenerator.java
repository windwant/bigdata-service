package org.windwant.bigdata.util;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.windwant.common.Constants;

import java.text.SimpleDateFormat;
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
    public static Logger logger = LoggerFactory.getLogger(LogGenerator.class);

    static {
//        Properties properties = new Properties();
//        properties.setProperty("log4j.rootLogger", "INFO,application,console,flume");
//        properties.setProperty("log4j.appender.application", "org.apache.log4j.DailyRollingFileAppender");
//        properties.setProperty("log4j.appender.application.File", "logs/test.log");
//        properties.setProperty("log4j.appender.application.Append", "true");
//        properties.setProperty("log4j.appender.application.DatePattern", "'.'yyyy-MM-dd");
//        properties.setProperty("log4j.appender.application.layout", "org.apache.log4j.PatternLayout");
//        properties.setProperty("log4j.appender.application.layout.ConversionPattern", "%d [%t] %-5p %C{6} (%F:%L) - %m%n");
//
//        properties.setProperty("log4j.appender.console", "org.apache.log4j.ConsoleAppender");
//        properties.setProperty("log4j.appender.console.Target", "System.out");
//        properties.setProperty("log4j.appender.console.layout", "org.apache.log4j.PatternLayout");
//        properties.setProperty("log4j.appender.console.layout.ConversionPattern", "%d [%t] %-5p %C{6} (%F:%L) - %m%n");
//
//        properties.setProperty("log4j.appender.flume", "org.apache.flume.clients.log4jappender.Log4jAppender");
//        properties.setProperty("log4j.appender.flume.hostname", "localhost");
//        properties.setProperty("log4j.appender.flume.port", "4444");
//        properties.setProperty("log4j.appender.flume.layout", "org.apache.log4j.PatternLayout");
//        properties.setProperty("log4j.appender.flume.layout.ConversionPattern", "%d [%t] %-5p %C{6} (%F:%L) - %m%n");
//        PropertyConfigurator.configure(properties);
    }

    public static void main(String[] args) {
        while (true) {
            logger.info("{} {} {}", genRandomIp(), genRandomRequest(), Constants.getRandomMethod());
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static StringBuilder ip = new StringBuilder();

    public static String genRandomIp() {
        return ip.delete(0, ip.length())
                .append(ThreadLocalRandom.current().nextInt(255))
                .append(".")
                .append(ThreadLocalRandom.current().nextInt(255))
                .append(".")
                .append(ThreadLocalRandom.current().nextInt(255))
                .append(".")
                .append(ThreadLocalRandom.current().nextInt(255))
                .toString();

    }
    static final String[] root = new String[]{"/login", "/logout", "/order", "/resource"};
    public static String genRandomPath(){
        return root[ThreadLocalRandom.current().nextInt(4)];
    }

    public static StringBuilder request = new StringBuilder();
    public static String genRandomRequest(){
        return request.delete(0, request.length())
                .append("http://")
                .append(genRandomIp())
                .append(genRandomPath())
                .toString();
    }

    public static SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static String getFormatDateTime(){
        return f.format(System.currentTimeMillis());
    }
}