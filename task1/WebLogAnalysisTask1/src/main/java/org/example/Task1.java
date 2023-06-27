package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task1 {

    public static class TaskOneMapper
            extends Mapper < Object, Text,  Text, IntWritable > {
        @Override
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {
            String tmp1[] = value.toString().split(" ");
            String remote_addr = tmp1[0];
            String remote_user = "-";
            tmp1 = value.toString().split("\\[");
            String tmp2[] = tmp1[1].split(" ");
            String time_local = tmp2[0];
            tmp1 = value.toString().split("\"");
            tmp2 = tmp1[2].split(" ");
            String status = tmp2[1];
            String body_bytes_sent = tmp2[2];
            tmp1 = (value.toString() + "remainder").split("\"");
            //Add a string in the end of value, or the empty string can not be split.
            String request;
            if(tmp1[1].length() <= 1)
                request = tmp1[1];
            else{
                tmp2 = tmp1[1].split(" ");
                request = tmp2[1];
            }

            String http_referer = tmp1[3];
            String http_user_agent = tmp1[5];
            /*
            * Some special char in url of request like #, ? and & need to be deleted.
            */
            tmp2 = request.split("\\?");
            request = tmp2[0];
            String res = remote_addr + "|" + remote_user + "|" + time_local + "|" + request + "|"
                    + status + "|" + body_bytes_sent + "|" + http_referer + "|" + http_user_agent + "|" + "remainder";
            //The remainder here has the same effect.
            context.write(new Text(res),new IntWritable(0));
        }
    }


    public static class TaskOneReducer
            extends Reducer < Text, IntWritable,Text, Text > {
        public void reduce(Text key, Iterable < IntWritable > values,
                           Context context
        ) throws IOException,
                InterruptedException {
            String res[] = key.toString().split("\\|");
            context.write(new Text("remote_addr:"),new Text(res[0]));
            context.write(new Text("remote_user:"),new Text(res[1]));
            context.write(new Text("time_local:"),new Text(res[2]));
            context.write(new Text("request:"),new Text(res[3]));
            context.write(new Text("status:"),new Text(res[4]));
            context.write(new Text("body_bytes_sent:"),new Text(res[5]));
            context.write(new Text("http_referer:"),new Text(res[6]));
            context.write(new Text("http_user_agent:"),new Text(res[7]));
            String tmp1[] = res[2].split("/");
            String tmp2[]  = tmp1[2].split(":");
            String month;
            switch(tmp1[1]){
                case "Jan":month = "01";break;
                case "Feb":month = "02";break;
                case "Mar":month = "03";break;
                case "Apr":month = "04";break;
                case "May":month = "05";break;
                case "Jun":month = "06";break;
                case "Jul":month = "07";break;
                case "Aug":month = "08";break;
                case "Sep":month = "09";break;
                case "Oct":month = "10";break;
                case "Nov":month = "11";break;
                case "Dec":month = "12";break;
                default:month = "-1";
            }
            String date = tmp2[0] + month + tmp1[0] + tmp2[1];
            context.write(new Text("date:"),new Text(date));
        }
    }




    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 3) { System.err.println("Usage: Task1 <in> <out>");
            System.exit(3);
        }
        Job job = Job.getInstance(conf, "Task1");
        job.setJarByClass(Task1.class);
        job.setMapperClass(TaskOneMapper.class);
        job.setReducerClass(TaskOneReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[2])))
            fs.delete(new Path(args[2]), true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

