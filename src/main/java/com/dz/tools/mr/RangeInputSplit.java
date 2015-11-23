package com.dz.tools.mr;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RangeInputSplit extends InputSplit implements Writable {

    private long start;
    private long stop;
    private long span;

    public RangeInputSplit() {
    }

    public RangeInputSplit(long start, long stop, long span) {
        this.start = start;
        this.span = span;
        this.stop=stop;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return 8*3;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeVLong(dataOutput, start);
        WritableUtils.writeVLong(dataOutput, stop);
        WritableUtils.writeVLong(dataOutput, span);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        start = WritableUtils.readVLong(dataInput);
        stop = WritableUtils.readVLong(dataInput);
        span = WritableUtils.readVLong(dataInput);
    }

    public long getSpan() {
        return span;
    }

    public long getStart() {
        return start;
    }

    public long getStop() {
        return stop;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RangeInputSplit that = (RangeInputSplit) o;

        if (start != that.start) return false;
        if (stop != that.stop) return false;
        return span == that.span;

    }

    @Override
    public int hashCode() {
        int result = (int) (start ^ (start >>> 32));
        result = 31 * result + (int) (stop ^ (stop >>> 32));
        result = 31 * result + (int) (span ^ (span >>> 32));
        return result;
    }
}