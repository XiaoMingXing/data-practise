package com.mapreduce2.triangles;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GraphEdgeMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

    private LongWritable k2 = new LongWritable();
    private LongWritable v2 = new LongWritable();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] splits = value.toString().split(",");
        long start = Long.parseLong(splits[0]);
        long end = Long.parseLong(splits[1]);
        k2.set(start);
        v2.set(end);
        System.out.println(String.format("Write Key value (%s,%s)", k2, v2));
        context.write(k2, v2);
        System.out.println(String.format("Write Key value (%s,%s)", v2, k2));
        context.write(v2, k2);
    }
}
