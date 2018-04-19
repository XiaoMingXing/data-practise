package com.mapreduce2.OrderInversion;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RelativeFrequencyCombiner extends Reducer<PairOfWords, IntWritable, PairOfWords, IntWritable> {

    // will merge the value for the same key before coming to reduce
    @Override
    public void reduce(PairOfWords key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int partialSum = 0;
        for (IntWritable value : values) {
            partialSum += value.get();
        }
        context.write(key, new IntWritable(partialSum));
    }
}