package org.windwant.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.net.URI;

/**
 *  bin/hadoop jar E:\javapro\bigdata-service\bigdata\target\bigdata-1.0-SNAPSHOT.jar org.windwant.bigdata.hadoop.ListFileStatus / /user/
 *  bin/hadoop jar E:\javapro\bigdata-service\bigdata\target\bigdata-1.0-SNAPSHOT.jar org.windwant.bigdata.hadoop.ListFileStatus hdfs://localhost:9000/ hdfs://localhost:9000/user/
 */
public class ListFileStatus {
    public static void main(String[] args) {
        if(args == null || args.length < 2) {
            args = new String[]{"hdfs://localhost:9000/", "hdfs://localhost:9000/test/hadoop/"};
        }
        String url = args[0];
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(url), new Configuration());
            Path[] path = new Path[args.length];
            for (int i = 0; i < path.length; i++) {
                path[i] = new Path(args[i]);
            }
            //筛选名称包含yarn的文件
            FileStatus[] status = fs.listStatus(path, new PathFilter() {
                public boolean accept(Path path) {
                    return path.getName().contains("yarn");
                }
            });
            for (FileStatus statu : status) {
                System.out.println(statu.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(fs != null){
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
