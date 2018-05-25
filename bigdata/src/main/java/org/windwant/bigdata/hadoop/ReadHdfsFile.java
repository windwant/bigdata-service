package org.windwant.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

/**
 * 执行文件读取
 * bin/hadoop jar hadoop-test-1.0-SNAPSHOT.jar org.windwant.bigdata.hadoop.ReadHdfsFile input/hadoop/capacity-scheduler.xm
 */
public class ReadHdfsFile {
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static String FILE_URL = "hdfs://192.168.7.138:50070/input/hadoop/capacity-scheduler.xml";
    public static void main(String[] args) {
        FILE_URL = args[0];
        InputStream in = null;
        FSDataInputStream fin = null;
        try{
            FileSystem fs = FileSystem.get(URI.create(FILE_URL), new Configuration());
            in = fs.open(new Path(FILE_URL));
            System.out.println("--------------------InputStream-------------------");
            IOUtils.copyBytes(in, System.out, 4096, false);
            fin = fs.open(new Path(FILE_URL));
            System.out.println("--------------------FSDataInputStream-------------------");
            IOUtils.copyBytes(fin, System.out, 4096, false);
            fin.seek(0);
            System.out.println("--------------------FSDataInputStream seek again-------------------");
            IOUtils.copyBytes(fin, System.out, 4096, false);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(fin);
        }
    }
}
