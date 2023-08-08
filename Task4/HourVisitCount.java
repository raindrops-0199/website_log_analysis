package HourVisitCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HourVisitCount {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "HourVisitCount");

        job.setJarByClass(HourVisitCount.class);
        job.setMapperClass(Task4Mapper.class);
        job.setCombinerClass(Task4Reducer.class);
        job.setReducerClass(Task4Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class Task4Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text date = new Text();
        private final IntWritable one = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String strVal = value.toString();
            if (strVal.startsWith("date")) {
                date.set(strVal.split("\t")[1]);
                context.write(date, one);
            }
        }
    }

    private static class Task4Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable value = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable item : values) {
                count += item.get();
            }
            value.set(count);
            context.write(key, value);
        }
    }
}
