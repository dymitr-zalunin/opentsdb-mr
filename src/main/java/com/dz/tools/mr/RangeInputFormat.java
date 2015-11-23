package com.dz.tools.mr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RangeInputFormat extends InputFormat<LongWritable, LongWritable> {

    private Log log = LogFactory.getLog(RangeInputFormat.class);

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        final int ROWS_PER_SPLIT = 1000;
        Configuration configuration = jobContext.getConfiguration();
        long start = configuration.getLong("start", 0);
        long stop = configuration.getLong("stop", 0);
        long span = configuration.getLong("span", 0);
        long rowsPerSplit = configuration.getLong("rows.per.map", ROWS_PER_SPLIT);
        log.info(String.format("Rows per split: %d", rowsPerSplit));
        long nSplits = ((stop - start) / (span * rowsPerSplit));
        long perSplit = rowsPerSplit * span;
        long current_start = start;
        List<InputSplit> splits = new ArrayList<InputSplit>();
        for (int i = 0; i < nSplits; i++) {
            splits.add(new RangeInputSplit(current_start, current_start+perSplit, span));
            current_start = current_start + perSplit;
        }
        if (current_start < stop) {
            splits.add(new RangeInputSplit(current_start, stop, span));
        }
        log.info(String.format("Splits: %d", splits.size()));
        return splits;
    }

    @Override
    public RecordReader<LongWritable, LongWritable> createRecordReader(InputSplit inputSplit,
                                                                       TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        RangeRecordReader rangeRecordReader = new RangeRecordReader();
        rangeRecordReader.initialize(inputSplit, taskAttemptContext);
        return rangeRecordReader;
    }

}
