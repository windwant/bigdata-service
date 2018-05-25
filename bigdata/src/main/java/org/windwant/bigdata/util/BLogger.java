package org.windwant.bigdata.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by Administrator on 2017/12/14.
 */
public class BLogger {
    public static Logger getLogger(Class clazz, String ...rollingFile){
        Properties properties = new Properties();
        properties.setProperty("log4j.rootLogger", "INFO,console");
        if(rollingFile.length >0){
            properties.setProperty("log4j.rootLogger", "INFO,console,application");
            properties.setProperty("log4j.appender.application", "org.apache.log4j.DailyRollingFileAppender");
            properties.setProperty("log4j.appender.application.File", rollingFile[0]);
            properties.setProperty("log4j.appender.application.Append", "true");
            properties.setProperty("log4j.appender.application.DatePattern", "'.'yyyy-MM-dd");
            properties.setProperty("log4j.appender.application.layout", "org.apache.log4j.PatternLayout");
            properties.setProperty("log4j.appender.application.layout.ConversionPattern", "%d [%t] %-5p %C{6} (%F:%L) - %m%n");
        }
        properties.setProperty("log4j.appender.console", "org.apache.log4j.ConsoleAppender");
        properties.setProperty("log4j.appender.console.Target", "System.out");
        properties.setProperty("log4j.appender.console.layout", "org.apache.log4j.PatternLayout");
        properties.setProperty("log4j.appender.console.layout.ConversionPattern", "%d [%t] %-5p %C{6} (%F:%L) - %m%n");
        return LoggerFactory.getLogger(clazz);
    }
}
