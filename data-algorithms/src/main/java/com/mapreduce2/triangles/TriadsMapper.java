package com.mapreduce2.triangles;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TriadsMapper extends Mapper<PairOfLongs, LongWritable, PairOfLongs, LongWritable> {

    @Override
    public void map(PairOfLongs key, LongWritable value, Context context)
            throws IOException, InterruptedException {
        context.write(key, value);
    }
}
