package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task2 {

    public static class TaskTwoMapper
            extends Mapper < Object, Text,  Text, IntWritable > {
        @Override
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {
            String v[] = value.toString().split("\t");
            if(v[0].equals("request:") && v.length == 2 && !(v[1].equals("-")))//"==" can not be used here as they have different reference.
                context.write(new Text(v[1]),new IntWritable(1));
        }
    }

    public static class TaskTwoCombiner extends Reducer < Text, IntWritable, Text, IntWritable > {
        public void reduce(Text key, Iterable < IntWritable > values,
                           Context context
        ) throws IOException,
                InterruptedException {
            int count = 0;
            for(IntWritable v:values){
                count += v.get();
            }
            context.write(key,new IntWritable(count));
        }
    }

    public static class TaskTwoReducer
            extends Reducer < Text, IntWritable,Text, IntWritable > {
        public void reduce(Text key, Iterable < IntWritable > values,
                           Context context
        ) throws IOException,
                InterruptedException {
            int count = 0;
            for(IntWritable v:values){
                count += v.get();
            }
            context.write(key,new IntWritable(count));
        }
    }




    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 3) { System.err.println("Usage: Task2 <in> <out>");
            System.exit(3);
        }
        Job job = Job.getInstance(conf, "Task2");
        job.setJarByClass(Task2.class);
        job.setMapperClass(TaskTwoMapper.class);
        job.setCombinerClass(TaskTwoCombiner.class);
        job.setReducerClass(TaskTwoReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[2])))
            fs.delete(new Path(args[2]), true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
