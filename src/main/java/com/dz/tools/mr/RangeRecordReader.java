package com.dz.tools.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class RangeRecordReader extends RecordReader<LongWritable, LongWritable> {

    private long start;
    private long stop;
    private long span;
    private long complete;
    private LongWritable key;
    private LongWritable value;

    public RangeRecordReader() {
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        start = ((RangeInputSplit) inputSplit).getStart();
        stop = ((RangeInputSplit) inputSplit).getStop();
        span = ((RangeInputSplit) inputSplit).getSpan();
        complete = start;
        value = new LongWritable(stop);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean next_exists = true;
        long current_start = complete;

        if (complete >= stop) {
            next_exists = false;
        } else if ((complete + span) > stop) {
            complete = stop;
        } else {
            complete += span;
        }
        key = new LongWritable(current_start);
        value = new LongWritable(complete);
        return next_exists;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public LongWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (stop - start == 0) {
            return 1;
        }
        return (complete - start) / (stop - start);
    }

    @Override
    public void close() throws IOException {
    }
}
