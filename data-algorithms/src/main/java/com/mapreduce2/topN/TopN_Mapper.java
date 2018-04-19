package com.mapreduce2.topN;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopN_Mapper extends Mapper<Text, IntWritable, Text, IntWritable> {

    @Override
    public void map(Text key, IntWritable value, Context context) {

    }
}
