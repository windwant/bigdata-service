package org.windwant.bigdata.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 执行文件次数统计
 * bin/hadoop jar bigdata-1.0-SNAPSHOT.jar org.windwant.bigdata.hadoop.WordCount input/hadoop output3
 */
public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                //收集输出
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static Job configJob(String in, String out) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        //处理Mapper
        job.setMapperClass(TokenizerMapper.class);

        //合并输出，减少mapper到reducer的数据传输
        job.setCombinerClass(IntSumReducer.class);
        //处理Reducer
        job.setReducerClass(IntSumReducer.class);
        //shezhi reducer数量 设置为0
        job.setNumReduceTasks(ThreadLocalRandom.current().nextInt(2));
        //输出 k-v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //任务输入
        FileInputFormat.addInputPath(job, new Path(in));
        //任务输出
        FileOutputFormat.setOutputPath(job, new Path(out));
        return job;
    }

    public static final String[] path = new String[]{"hdfs://localhost:9000/test/hadoop/capacity-scheduler.xml", "hdfs://localhost:9000/test/out/"};

    public static void main(String[] args) throws Exception {
        if(args == null || args.length < 2){
            args = path;
        }
        args[1] = args[1] + System.currentTimeMillis();

        Job job = configJob(args[0], args[1]);
        //等待任务结束
        if(job.waitForCompletion(true)){
            //结果显示
            String result = job.getNumReduceTasks() == 0?"/part-m-00000":"/part-r-00000";
            HdfsFileOp.readHdfsFile(args[1] + result);
            System.exit(0);
        }
        System.exit(1);
    }
}