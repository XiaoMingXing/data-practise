package com.mapreduce2.triangles;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class TriadsReducer extends Reducer<PairOfLongs, LongWritable, Text, Text> {

    private final Text EMPTY = new Text();

    @Override
    public void reduce(PairOfLongs key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        List<LongWritable> valueList = newArrayList(values);
        if (valueList.size() > 1 && valueList.contains(0)) {
            for (LongWritable node : valueList) {
                if (node.get() != 0) {
                    Text triangle = new Text();
                    triangle.set(String.format("%s,%s,%s", key.getLeft(), key.getRight(), node.get()));
                    context.write(triangle, EMPTY);
                }
            }
        }
    }
}
