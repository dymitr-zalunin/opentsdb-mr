package com.dz.tools.mr;

import com.dz.tools.ArgsP;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;

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
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        return job.waitForCompletion(true) ? 1 : 0;
    }

    public static void usage() {
        System.err.println("Usage:java -jar -cp:<class_path> com.dz.tools.TimeSeriesGenerator <params>");
        System.err.println("<params>: <name> <start> <stop> <span> [<tagk1>=<tagv1> <tagk2>=<tagv2>...]");
        System.exit(-1);
    }

    public static int main(String[] args) throws Exception {
        int run = -1;
        run = ToolRunner.run(new MRGenerator(), args);
        return run;
    }

}
