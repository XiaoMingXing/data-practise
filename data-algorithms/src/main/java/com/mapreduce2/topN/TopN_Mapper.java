package com.mapreduce2.topN;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

public class TopN_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    // TreeMap is unsynchronized collection class which means it is not suitable for thread-safe operations until unless synchronized explicitly.
    private final TreeMap<Integer, String> treeMap = new TreeMap<>();
    private int top_n = 5;


    @Override
    public void setup(Context context) {
        top_n = context.getConfiguration()
                .getInt("top.n", 5);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) {
        String[] tokens = value.toString().split(",");
        if (tokens.length < 2) {
            return;
        }
        int weight = Integer.parseInt(tokens[0]);
        if (treeMap.size() < top_n) {
            treeMap.put(weight, tokens[1]);
        } else {
            treeMap.remove(weight);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (int weight : treeMap.keySet()) {
            Text key = new Text(treeMap.get(weight));
            context.write(key, new DoubleWritable(weight));
        }
    }
}
