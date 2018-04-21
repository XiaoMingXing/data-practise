package com.mapreduce2.topN;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class TopN_Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private final TreeMap<Double, String> treeMap = new TreeMap<>();
    private int top_n = 5;


    @Override
    public void setup(Context context) {
        top_n = context.getConfiguration().getInt("top.n", 5);
    }

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) {
        values.forEach(value -> {
            saveToTreeMap(key.toString(), value.get());
        });
    }

    private void saveToTreeMap(String key, double value) {
        treeMap.put(value, key);
        if (treeMap.size() > top_n) {
            treeMap.pollFirstEntry();
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (double weight : treeMap.keySet()) {
            Text key = new Text(treeMap.get(weight));
            context.write(key, new DoubleWritable(weight));
        }
    }
}
