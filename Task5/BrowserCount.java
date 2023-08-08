package org.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import java.util.HashSet;
import java.util.Set;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;




public class BrowserCount {


    private static class Task5Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("cyp");//根据分隔符cyp将输入分割为9部分
            String[] browser0 = splits[7].split("\t");//取浏览器部分
            String browser = browser0[1];//取浏览器字符串
            String ip = splits[0].split("\t")[1];//取用户IP地址
            context.write(new Text(browser), new Text(ip));//以浏览器为键，Ip地址为值传给reducer
            
        }
    }

    private static class Task5Reducer extends Reducer<Text, Text, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> Ip = new HashSet<>();//初始化一个哈希表
            for (Text text : values) {
                Ip.add(text.toString());//将ip地址添加进哈希表
            }
            context.write(key, new IntWritable(Ip.size()));//哈希表的大小就是用户的数量
        }
    }
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BrowserCount");
        job.setJarByClass(BrowserCount.class);
        job.setInputFormatClass(NewInputFormat.class);
        job.setMapperClass(Task5Mapper.class);
        job.setReducerClass(Task5Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}