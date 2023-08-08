package IPCount;

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
import java.util.HashSet;
import java.util.Set;

public class IPCount {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "IPCount");
        job.setJarByClass(IPCount.class);
        job.setMapperClass(Task3Mapper.class);
        job.setReducerClass(Task3Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(ResInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class Task3Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text resource = new Text();
        private Text IpText = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(";");
            String[] requestF = splits[3].split("\t");
            if (!(requestF.length > 1)) {
                return;
            }
            String request = requestF[1];
            if (!request.equals("-") && !request.equals("/")) {
                String ip = splits[0].split("\t")[1];
                resource.set(request);
                IpText.set(ip);
                context.write(resource, IpText);
            }
        }
    }

    private static class Task3Reducer extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable outVal = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> IpSet = new HashSet<>();
            for (Text text : values) {
                IpSet.add(text.toString());
            }
            outVal.set(IpSet.size());
            context.write(key, outVal);
        }
    }
}
