package org.windwant.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * hdfs 添加文件 文件复制 文件夹复制
 *
 * 创建文件夹：bin/hdfs dfs -mkdir /test
 * 查看文件夹：bin/hdfs dfs -ls /test
 *
 * hadoop路径：hdfs://localhost:9000/test
 *
 * bin/hadoop jar bigdata-1.0-SNAPSHOT.jar org.windwant.bigdata.hadoop.WriteHdfsFile etc/hadoop/capacity-scheduler.xml input/test/cs.xml
 * bin/hadoop jar bigdata-1.0-SNAPSHOT.jar org.windwant.bigdata.hadoop.WriteHdfsFile etc/hadoop/capacity-scheduler.xml hdfs://localhost:9000/user/roger/haddop/cs.xml
 */
public class WriteHdfsFile {
    //file copy
    public static final String[] filePath = new String[]{"D:\\abc.txt", "hdfs://localhost:9000/test/abc.java"};
    //dir copy
    public static final String[] dirPath = new String[]{"D:\\hadoop-2.9.0\\etc\\hadoop", "hdfs://localhost:9000/test/tmp"};

    public static void main(String[] args) {
        if(args == null || args.length < 2){
            args = dirPath;
        }

        copyFileToHdfs(new File(args[0]), args[1]);
    }

    public static void copyFileToHdfs(File src, String des){
        try {
            if(src.isDirectory()){
                File[] files = src.listFiles();
                FileSystem fs = FileSystem.get(URI.create(des), new Configuration());
                if(!fs.exists(new Path(des))) {
                    fs.mkdirs(new Path(des), new FsPermission((short) 777));
                }
                for (File f : files) {
                    copyFileToHdfs(f, des + "/" + f.getName());
                }
            }else {
                InputStream in = null;

                in = new BufferedInputStream(new FileInputStream(src));
                FileSystem fs = FileSystem.get(URI.create(des), new Configuration());
                System.out.println("file: " + src);
                FSDataOutputStream out = fs.create(new Path(des), new Progressable() {
                    Integer times = 1;

                    public void progress() {
                        System.out.println(getDotDot(times++));
                    }

                    private String getDotDot(Integer lgn) {
                        String dotStr = "";
                        for (Integer i = 0; i < lgn; i++) {
                            dotStr += ".";
                        }
                        return dotStr;
                    }
                });
                IOUtils.copyBytes(in, out, 4096, true);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        } finally {
//            IOUtils.closeStream(in);
//        }
    }
}
