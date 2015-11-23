package com.dz.tools;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class TimeSeries {

    private String metric_name;
    private Map<String, String> tags;
    private List<DataPoint> data_points;

    public TimeSeries(String metric_name) {
        this.metric_name = metric_name;
        this.tags = Maps.newHashMap();
        this.data_points= Lists.newArrayList();
    }

    public void addTag(String tag, String value) {
        this.tags.put(tag, value);
    }

    public void addDataPoint(DataPoint data_point) {
        data_points.add(data_point);
    }

    public String getMetric_name() {
        return metric_name;
    }

    public List<DataPoint> getData_points() {
        return data_points;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public static class DataPoint {
        public double value;
        public long timestamp;

        public DataPoint(long timestamp, double value) {
            this.timestamp = timestamp;
            this.value = value;
        }

        @Override
        public String toString() {
            return Long.toString(timestamp);
        }
    }

}
