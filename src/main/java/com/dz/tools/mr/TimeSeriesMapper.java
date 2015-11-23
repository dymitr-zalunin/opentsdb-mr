package com.dz.tools.mr;

import com.dz.tools.TimeSeries;
import com.dz.tools.TimeSeriesGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TimeSeriesMapper extends Mapper<LongWritable, LongWritable, ImmutableBytesWritable, Put> {

    private final static Log log = LogFactory.getLog(TimeSeriesMapper.class);

    private Map<String, String> tags;
    private HTablePool hTablePool;
    private Long span;
    private String mertic_name;

    private TimeSeriesGenerator timeSeriesGenerator;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        tags = parseTags(configuration.get("tags"));
        hTablePool = new HTablePool();
        timeSeriesGenerator = new TimeSeriesGenerator();
        span = configuration.getLong("span", 1);
        mertic_name = configuration.get("metric");
        log.info("Initialized the mapper");
    }

    @Override
    protected void map(LongWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
        long start = key.get();
        long end = value.get();context.progress();
        log.info(String.format("start=%d, stop=%d, span=%d", start, end, span));
        TimeSeries timeSeries = timeSeriesGenerator.generateTimeSeries(start, end, span, mertic_name, tags);
        List<Put> entries = timeSeriesGenerator.createOpenTSDBPoints(timeSeries, hTablePool);
        log.info(String.format("Generated %d points for time series %s",
                entries.size(), timeSeries.getMetric_name()));
        for (Put entry : entries) {
            context.write(new ImmutableBytesWritable(entry.getRow()), entry);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        hTablePool.close();
    }



    private Map<String, String> parseTags(String args) {
        String[] tags = args.split(",");
        Map<String, String> result = new HashMap<String, String>();
        for (String tag : tags) {
            String[] tagVK = tag.split("=");
            result.put(tagVK[0], tagVK[1]);
        }
        return result;
    }
}
