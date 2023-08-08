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


    private static class Task6Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text Code = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("cyp");//将数据切分为九行
            String[] code0 = splits[4].split("\t");//读取请求状态码
            String code = code0[1];
            String ip = splits[0].split("\t")[1];
            Code.set(code);
            context.write(Code, new IntWritable(1));//key设置为请求状态码
            
        }
    }

    private static class Task6Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable text : values) {
                count++;
            }
            context.write(key, new IntWritable(count));
        }
    }
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BrowserCount");
        job.setJarByClass(BrowserCount.class);
        job.setInputFormatClass(NewInputFormat.class);
        job.setCombinerClass(Task6Reducer.class)
        job.setMapperClass(Task6Mapper.class);
        job.setReducerClass(Task6Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}