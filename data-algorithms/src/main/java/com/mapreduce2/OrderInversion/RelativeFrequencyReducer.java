package com.mapreduce2.OrderInversion;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RelativeFrequencyReducer extends Reducer<PairOfWords, IntWritable, PairOfWords, DoubleWritable> {

    private double totalCount = 0;
    private String currentWord = "NOT_DEFINED";
    private final DoubleWritable relativeCount = new DoubleWritable();

    // this reducer need to promise that the order of the records
    @Override
    public void reduce(PairOfWords key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        if (key.getRightElement().equals("*")) {
            if (key.getLeftElement().equals(this.currentWord)) {
                this.totalCount += getTotalCount(values);
            } else {
                this.currentWord = key.getLeftElement();
                this.totalCount = getTotalCount(values);
            }
        } else {
            int count = getTotalCount(values);
            relativeCount.set(count / totalCount);
            context.write(key, relativeCount);
        }
    }

    private int getTotalCount(Iterable<IntWritable> values) {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        return sum;
    }
}
