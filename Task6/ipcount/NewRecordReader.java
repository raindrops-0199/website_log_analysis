package org.example;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class ResRecordReader extends LineRecordReader {
    private Text resValue = new Text();
    private LongWritable resKey = new LongWritable();

    @Override
    public boolean nextKeyValue() throws IOException {
        StringBuilder valueBuilder = new StringBuilder();
        int count = 0;
        while (super.nextKeyValue()) {
            LongWritable key = super.getCurrentKey();
            Text value = super.getCurrentValue();
            resKey.set(key.get());
            valueBuilder.append(value.toString()).append("cyp");
            count++;
            if (count == 9) {
                resValue.set(valueBuilder.toString());
                return true;
            }
        }
        if (count > 0) {
            resValue.set(valueBuilder.toString());
            return true;
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() {
        return resKey;
    }

    @Override
    public Text getCurrentValue() {
        return resValue;
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        super.initialize(genericSplit, context);
    }

    @Override
    public float getProgress() throws IOException {
        return super.getProgress();
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}