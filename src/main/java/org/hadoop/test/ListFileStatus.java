package org.hadoop.test;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.net.URI;

/**
 *  bin/hadoop jar E:\javapro\hadoop-test\target\hadoop-test-1.0-SNAPSHOT.jar org.hadoop.test.ListFileStatus / /user/
 *  bin/hadoop jar E:\javapro\hadoop-test\target\hadoop-test-1.0-SNAPSHOT.jar org.hadoop.test.ListFileStatus hdfs://localhost:9000/ hdfs://localhost:9000/user/
 */
public class ListFileStatus {
    public static void main(String[] args) {
        String url = args[0];
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(url), new Configuration());
            Path[] path = new Path[args.length];
            for (int i = 0; i < path.length; i++) {
                path[i] = new Path(args[i]);
            }
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
        }

    }
}
