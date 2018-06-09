package com.mapreduce2.triangles;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class GraphEdgeReducer extends Reducer<LongWritable, LongWritable, PairOfLongs, LongWritable> {

    PairOfLongs k2 = new PairOfLongs();
    LongWritable v2 = new LongWritable();


    private String getValue(Iterable<LongWritable> ends) {
        StringBuffer stringBuffer = new StringBuffer();
        ends.forEach((value -> {
            stringBuffer.append(value + ",");
        }));
        return stringBuffer.toString();
    }

    @Override
    public void reduce(LongWritable start, Iterable<LongWritable> ends, Context context)
            throws IOException, InterruptedException {
        v2.set(0);
        for (LongWritable end : ends) {
            k2.set(start.get(), end.get());
            context.write(k2, v2);
        }

        List<LongWritable> nodes = newArrayList(ends);
        System.out.println(String.format("[Reducer] Receive the record %s:(%s)", start, getValue(nodes)));
        Collections.sort(nodes);
        for (int index = 0; index < nodes.size() - 1; index++) {
            long nodeA = nodes.get(index).get();
            long nodeB = nodes.get(index + 1).get();
            k2.set(nodeA, nodeB);
            v2.set(start.get());
            context.write(k2, v2);
        }
    }
}
