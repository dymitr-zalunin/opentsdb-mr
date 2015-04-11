package com.dz.tools;

import com.dz.tools.TimeSeries.DataPoint;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class TimeSeriesGenerator {

    public static final byte[] TSDB_TABLE_NAME = Bytes.toBytes("tsdb");
    public static final byte[] T_CF = Bytes.toBytes("t");
    //tsdb-uid table
    public static final byte[] TSDB_UID_TABLE = Bytes.toBytes("tsdb-uid");
    public static final byte[] ID_TSDB_UID_CF = Bytes.toBytes("id");
    public static final byte[] NAME_TSDB_UID_CF = Bytes.toBytes("name");
    public static final byte[] TAGV = Bytes.toBytes("tagv");
    public static final byte[] TAGK = Bytes.toBytes("tagk");
    public static final byte[] METRICS = Bytes.toBytes("metrics");

    private static final byte[] MAXID_ROW = {0};
    public static final int ID_WIDTH = 3;
    public static final int TIMESTAMP_BYTES = 4;
    public static final int FLAG_BITS = 4;
    public static final int MAX_TIMESPAN = 3600;

    public TimeSeriesGenerator() {
    }

    public List<Put> createOpenTSDBPoints(TimeSeries time_series, HTablePool pool) throws IOException {
        List<Put> dpEntries=new ArrayList<Put>();
        HTableInterface  uid_table= pool.getTable(TSDB_UID_TABLE);
        byte[] row_key = rowKeyTemplate(time_series.getMetric_name(), time_series.getTags(), uid_table);
        final short FLOAT_FLAG = 0x8;
        final short flags = FLOAT_FLAG | 0x7;
        for (DataPoint dataPoint : time_series.getData_points()) {
            long base_time = dataPoint.timestamp - (dataPoint.timestamp % MAX_TIMESPAN);
            byte[] qualifier = buildQuialifier(dataPoint.timestamp, flags);
            System.arraycopy(Bytes.toBytes(((int) base_time)), 0, row_key, ID_WIDTH, TIMESTAMP_BYTES);
            Put p = new Put(row_key);
            p.add(T_CF, qualifier, Bytes.toBytes(dataPoint.value));
            dpEntries.add(p);
        }
        return dpEntries;
    }

    private byte[] rowKeyTemplate(String metric, Map<String, String> tags, HTableInterface uid_table) throws IOException {
        byte[] metric_id = getOrCreateId(metric, METRICS,uid_table);
        int tags_size = tags.size();
        int row_size = metric_id.length + TIMESTAMP_BYTES + tags_size * ID_WIDTH + tags_size * ID_WIDTH;
        byte[] row_key = new byte[row_size];
        //<metric_uid><timestamp><tagk1><tagv1>[...<tagkN><tagvN>]
        int pos = 0;
        System.arraycopy(metric_id, 0, row_key, pos, metric_id.length);
        pos += metric_id.length + TIMESTAMP_BYTES;
        for (Map.Entry<String, String> tag : tags.entrySet()) {
            byte[] tagk_id = getOrCreateId(tag.getKey(), TAGK,uid_table);
            byte[] tagv_id = getOrCreateId(tag.getValue(), TAGV,uid_table);
            System.arraycopy(tagk_id, 0, row_key, pos, tagk_id.length);
            pos += tagv_id.length;
            System.arraycopy(tagv_id, 0, row_key, pos, tagv_id.length);
            pos += tagv_id.length;
        }
        return row_key;
    }

    private byte[] buildQuialifier(long timestamp, short flags) {
        long base_time = timestamp - (timestamp % MAX_TIMESPAN);
        final short qualifier = (short) ((timestamp - base_time) << FLAG_BITS | flags);
        return Bytes.toBytes(qualifier);
    }

    private byte[] getOrCreateId(String name, byte[] kind, HTableInterface uid_table) throws IOException {
        byte[] name_bytes = Bytes.toBytes(name);
        Get get_uid = new Get(name_bytes);
        get_uid.addColumn(ID_TSDB_UID_CF, kind);
        Result uid_result = uid_table.get(get_uid);
        byte[] uid_bytes = null;
        if (!uid_result.isEmpty()) {
            uid_bytes = uid_result.getColumnLatest(ID_TSDB_UID_CF, kind).getValue();
        } else {
            long uid = uid_table.incrementColumnValue(MAXID_ROW, ID_TSDB_UID_CF, kind, 1);
            uid_bytes = Bytes.toBytes(uid);
            uid_bytes = Arrays.copyOfRange(uid_bytes, uid_bytes.length - ID_WIDTH, uid_bytes.length);
            Put revers_mapping_put = new Put(uid_bytes);
            revers_mapping_put.add(NAME_TSDB_UID_CF, kind, name_bytes);
            uid_table.put(revers_mapping_put);
            Put forward_mapping_put = new Put(name_bytes);
            forward_mapping_put.add(ID_TSDB_UID_CF, kind, uid_bytes);
            uid_table.put(forward_mapping_put);
        }
        return uid_bytes;
    }

    /**
     * Generates time series with values from [0, 100] range.
     *
     * @param start start time in seconds for time series
     * @param stop end time in seconds for time series
     * @param span span between two data points in seconds
     * @param metric_name name of metric
     * @param tags {@link java.util.Map<java.lang.String,java.lang.String>} map with tags
     *             where key - tag name, value - tag value
     * @return {@link TimeSeries}
     */
    public TimeSeries generateTimeSeries(long start, long stop, long span, String metric_name, Map<String, String> tags) {
        TimeSeries timeSeries = new TimeSeries(metric_name);
        for (Map.Entry<String, String> tag : tags.entrySet()) {
            timeSeries.addTag(tag.getKey(), tag.getValue());
        }
        Random random = new Random();
        for (long i = start; i < stop; i+=span) {
            timeSeries.addDataPoint(new DataPoint(i, random.nextDouble() * 100));
        }

        return timeSeries;
    }

    public void run(String[] args) throws IOException {
        ArgsP argsP = new ArgsP(args);
        String metric_name = argsP.getMetricName();
        long start = argsP.getStart().getMillis() / 1000;
        long stop = argsP.getStop().getMillis() / 1000;
        long span = argsP.getSpan();
        Map<String, String> tags = argsP.getTags();
        TimeSeries timeSeries = generateTimeSeries(start, stop, span, metric_name, tags);

        HTablePool hTablePool = new HTablePool();
        TimeSeriesGenerator timeSeriesGenerator = new TimeSeriesGenerator();
        List<Put> dpEntries = timeSeriesGenerator.createOpenTSDBPoints(timeSeries, hTablePool);
        HTableInterface tsdbTable = hTablePool.getTable(TSDB_TABLE_NAME);
        tsdbTable.put(dpEntries);
        tsdbTable.close();
        hTablePool.close();
    }

    public static void usage() {
        System.err.println("Usage:java -jar -cp:<class_path> com.dz.tools.TimeSeriesGenerator <params>");
        System.err.println("<params>: <name> <start> <stop> <span> [<tagk1>=<tagv1> <tagk2>=<tagv2>...]");
        System.exit(-1);
    }

    public static void main(String[] args) throws IOException {
        try {
            TimeSeriesGenerator timeSeriesGenerator = new TimeSeriesGenerator();
            timeSeriesGenerator.run(args);
        } catch (IllegalArgumentException e) {
            usage();
        }
    }
}
