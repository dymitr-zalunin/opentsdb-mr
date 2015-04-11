package com.dz.tools;

import com.dz.tools.mr.MRGenerator;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

import java.io.IOException;

public class TimeSeriesGeneratorRunner {

    public static void usage() {
        System.err.println("Usage:java -jar -cp:<class_path> com.dz.tools.TimeSeriesGeneratorRunner <mode> <params>");
        System.err.println("<mode>: mr | sa");
        System.err.println("mr - run time series generator as MapReduce Job");
        System.err.println("sa - run time series generator as stadnlone");
        System.err.println("<params>: <name> <start> <stop> <span> [<tagk1>=<tagv1> <tagk2>=<tagv2>...]");
        System.exit(-1);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length==0) {
            usage();
        }
        String mode = args[0];
        ArgsP argsP = new ArgsP(rest(args));
        try {
            if ("mr".equals(mode)) {
                MRGenerator timeSeriesDriver=new MRGenerator();
                timeSeriesDriver.run(args);
            } else if ("sa".equals(mode)) {
                TimeSeriesGenerator timeSeriesGenerator = new TimeSeriesGenerator();
                timeSeriesGenerator.run(args);
            }
        }catch (IllegalArgumentException e) {
            usage();
        }
    }

    private static String[] rest(String[] args) {
        return FluentIterable.from(Lists.newArrayList(args)).toList().subList(1, args.length)
                .toArray(new String[]{});
    }
}