package com.dz.tools.mr;

import com.dz.tools.ArgsP;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

public class MRGenerator extends Configured implements Tool {

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = getConf();
        ArgsP argsP = new ArgsP(args);
        long start = argsP.getStart().getMillis() / 1000;
        long stop = argsP.getStop().getMillis() / 1000;
        long span = argsP.getSpan();
        String metric_name = argsP.getMetricName();


        String[] tags = FluentIterable.from(argsP.getTags().entrySet())
                .transform(new Function<Map.Entry<String, String>, String>() {
                    @Override
                    public String apply(Map.Entry<String, String> input) {
                        return input.getKey() + "=" + input.getValue();
                    }
                }).toArray(String.class);
        conf.setLong("start", start);
        conf.setLong("stop", stop);
        conf.setLong("span", span);
        conf.setStrings("tags", tags);
        conf.set("metric", metric_name);
        conf.set(TableOutputFormat.OUTPUT_TABLE, "tsdb");
        Job job = new Job(conf);
        job.setJarByClass(MRGenerator.class);
        job.setMapperClass(TimeSeriesMapper.class);
        job.setInputFormatClass(RangeInputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        Connection connection = ConnectionFactory.createConnection(conf);
        Table hTable = connection.getTable(TableName.valueOf("tsdb"));
        HFileOutputFormat2.setOutputPath(job, new Path(metric_name));
        HFileOutputFormat2.configureIncrementalLoad(job, hTable,connection.getRegionLocator(TableName.valueOf("tsdb")));

        int res = job.waitForCompletion(true) ? 1 : 0;
        return res;
    }

    public static void usage() {
        System.err.println("Usage:java -jar -cp:<class_path> com.dz.tools.TimeSeriesGenerator <params>");
        System.err.println("<params>: <name> <start> <stop> <span> [<tagk1>=<tagv1> <tagk2>=<tagv2>...]");
        System.exit(-1);
    }

    public static int main(String[] args) throws Exception {
        int run = -1;
        run = ToolRunner.run(HBaseConfiguration.create(), new MRGenerator(), args);
        return run;
    }

}
