package com.dz.tools;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.Map;

public class ArgsP {
    private String[] args;

    public ArgsP(String[] args) {
        this.args = args;
    }

    public String getMetricName() {
        usage_with_error();
        return args[0];
    }

    public DateTime getStart() {
        usage_with_error();
        DateTime dateTime = formatter().parseDateTime(args[1]);
        return dateTime;
    }

    public DateTime getStop() {
        usage_with_error();
        DateTime dateTime = formatter().parseDateTime(args[2]);
        return dateTime;
    }

    public long getSpan() {
        usage_with_error();
        return Long.parseLong(args[3]);
    }

    public Map<String, String> getTags() {
        Map<String, String> tags = new HashMap<String, String>();
        for (int i = 4; i < args.length; i++) {
            String[] tag=args[i].split("=");
            tags.put(tag[0],tag[1]);
        }
        return tags;
    }

    private DateTimeFormatter formatter() {
        return DateTimeFormat.forPattern("YYYY-MM-dd'T'HH:mm:ss");

    }

    private void usage_with_error() {
        if (args.length<1) {
            System.err.println("missing metric name");
            throw new IllegalArgumentException();
        }

        if (args.length<2) {
            System.err.println("missing start time. Example 2011-01-22T00:00:00");
            throw new IllegalArgumentException();
        }

        if (args.length<3) {
            System.err.println("missing stop time. Example 2011-01-22T00:00:00");
            throw new IllegalArgumentException();
        }

        if (args.length<4) {
            System.err.println("missing span time between data points ");
            throw new IllegalArgumentException();
        }
    }

}
