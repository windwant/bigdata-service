package org.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * hdfs 添加文件
 * bin/hadoop jar hadoop-test-1.0-SNAPSHOT.jar org.hadoop.test.WriteHdfsFile etc/hadoop/capacity-scheduler.xml input/test/cs.xml
 * bin/hadoop jar hadoop-test-1.0-SNAPSHOT.jar org.hadoop.test.WriteHdfsFile etc/hadoop/capacity-scheduler.xml hdfs://localhost:9000/user/roger/haddop/cs.xml
 */
public class WriteHdfsFile {
    public static void main(String[] args) {
        String localSrc = args[0];
        String des = args[1];
        InputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(localSrc));
            FileSystem fs = FileSystem.get(URI.create(des), new Configuration());
            FSDataOutputStream out = fs.create(new Path(des), new Progressable() {
                Integer times = 1;
                public void progress() {
                    System.out.println(getDotDot(times++));
                }

                private String getDotDot(Integer lgn){
                    String dotStr = "";
                    for (Integer i = 0; i < lgn; i++) {
                        dotStr += ".";
                    }
                    return dotStr;
                }
            });
            IOUtils.copyBytes(in, out, 4096, false);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
