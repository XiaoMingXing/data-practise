package com.mapreduce2.markov;

import org.apache.hadoop.mapreduce.Partitioner;

import javax.xml.soap.Text;

public class SecondarySortPartitioner extends Partitioner<CompositeKey, Text> {
    @Override
    public int getPartition(CompositeKey compositeKey, Text text, int numberOfPartitions) {
        return Math.abs((int) hash(compositeKey.getCustomerId()) % numberOfPartitions);
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
