package com.mapreduce2.movingAverage;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimeSeriesData implements Writable, Comparable<TimeSeriesData> {

    private long timestamp;
    private double value;

    public TimeSeriesData() {
    }

    public TimeSeriesData(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public int compareTo(TimeSeriesData o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.timestamp);
        dataOutput.writeDouble(this.value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.timestamp = dataInput.readLong();
        this.value = dataInput.readDouble();
    }

    public static TimeSeriesData copy(TimeSeriesData tsd) {
        return new TimeSeriesData(tsd.getTimestamp(), tsd.getTimestamp());
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
