package com.mapreduce2.topN;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopN_Reducer extends Reducer<NullWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context) {

    }
}
