package com.mapreduce2.OrderInversion;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderInversionPartitioner extends Partitioner<PairOfWords, IntWritable> {
    @Override
    public int getPartition(PairOfWords key, IntWritable intWritable, int num) {
        // Todo why use this?
        return Math.abs((int) hash(key.getLeftElement()) % num);
    }

    private static long hash(String str) {
        long h = 1125899906842597L;
        int length = str.length();
        for (int index = 0; index < length; index++) {
            h = 31 * h + str.charAt(index);
        }
        return h;
    }
}
