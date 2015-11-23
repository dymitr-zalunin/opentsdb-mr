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
    private boolean isNext;
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
        isNext=true;
        value = new LongWritable(stop);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        key = new LongWritable(start);
        value = new LongWritable(stop);
        boolean next=false;
        if (isNext) {
            next=true;
            isNext=false;
        }
        return next;
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
        return 1;
    }

    @Override
    public void close() throws IOException {
    }
}
