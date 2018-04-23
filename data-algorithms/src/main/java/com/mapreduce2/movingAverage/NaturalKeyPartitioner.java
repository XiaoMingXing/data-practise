package com.mapreduce2.movingAverage;


import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKey, TimeSeriesData> {
    @Override
    public int getPartition(CompositeKey compositeKey, TimeSeriesData timeSeriesData, int numberOfPartitions) {
        return Math.abs((int) hash(compositeKey.getName()) % numberOfPartitions);
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
