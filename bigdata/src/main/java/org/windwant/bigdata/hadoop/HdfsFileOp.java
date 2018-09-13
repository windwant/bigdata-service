package org.windwant.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;

/**
 * Created by Administrator on 18-9-10.
 */
public class HdfsFileOp {
    //file copy
    public static final String[] filePath = new String[]{"D:\\abc.txt", "hdfs://localhost:9000/test/abc.java"};
    //dir copy
    public static final String[] dirPath = new String[]{"D:\\hadoop-2.9.0\\etc\\hadoop", "hdfs://localhost:9000/test/tmp"};

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
     *
     * @param src
     * @param des
     */
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
    }

    private static final String[] listPath =  new String[]{"hdfs://localhost:9000/", "hdfs://localhost:9000/test/hadoop/"};

    /**
     * 文件查看 筛选
     * @param paths
     * @param filter
     */
    public static void listFileStatus(String[] paths, String filter){
        if(paths == null || paths.length == 0){
            return;
        }
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(paths[0]), new Configuration());
            Path[] path = new Path[paths.length];
            for (int i = 0; i < path.length; i++) {
                path[i] = new Path(paths[i]);
            }
            //筛选名称包含yarn的文件
            FileStatus[] status = fs.listStatus(path, new PathFilter() {
                public boolean accept(Path path) {
                    return path.getName().contains(filter);
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

    public static String FILE_URL = "hdfs://localhost:9000/test/hadoop/capacity-scheduler.xml";

    /**
     * 文件读取
     * @param path
     */
    public static void readHdfsFile(String path){
        if(path != null) {
            FILE_URL = path;
        }
        InputStream in = null;
        FSDataInputStream fin = null;
        try{
            FileSystem fs = FileSystem.get(URI.create(FILE_URL), new Configuration());
            in = fs.open(new Path(FILE_URL));
            System.out.println("--------------------InputStream-------------------");
            IOUtils.copyBytes(in, System.out, 4096, false);
//            fin = fs.open(new Path(FILE_URL));
//            System.out.println("--------------------FSDataInputStream-------------------");
//            IOUtils.copyBytes(fin, System.out, 4096, false);
//            fin.seek(0);
//            System.out.println("--------------------FSDataInputStream seek again-------------------");
//            IOUtils.copyBytes(fin, System.out, 4096, false);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(fin);
        }
    }

    /**
     * 删除 文件 文件夹
     * @param path
     */
    public static void deleteFile(String path){
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(path), new Configuration());
            fs.delete(new Path(path), true);
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

    private static final Logger logger = LoggerFactory.getLogger(HdfsFileOp.class);
    public static void main(String[] args) {
        readHdfsFile("hdfs://localhost:9000/test/hadoop/core-site.xml");
        logger.info("read hdfs file content");
    }
}
