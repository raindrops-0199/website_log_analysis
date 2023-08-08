package org.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import java.util.HashSet;
import java.util.Set;
import java.io.IOException;





public class BrowserCount {


    private static class Task6Mapper extends Mapper<Text, Text, IntWritable, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String num = value.toString();
            int n = Integer.parseInt(num);
            context.write(new IntWritable(n), key);
        }
    }

    private static class Task6Reducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        @Override
        public void reduce(IntWritable num, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values)
            {
                context.write(v, num);
            }
        }
    }
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BrowserCount");
        job.setJarByClass(BrowserCount.class);
        job.setMapperClass(Task6Mapper.class);
        job.setReducerClass(Task6Reducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}