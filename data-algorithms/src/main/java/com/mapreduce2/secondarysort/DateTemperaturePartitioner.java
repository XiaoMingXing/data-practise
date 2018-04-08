package com.mapreduce2.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


//  Set the Partitioner class used to partition Mapper-outputs to be sent to the Reducers.
public class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair, Text> {
    public int getPartition(DateTemperaturePair dateTemperaturePair, Text text, int numberOfPartitions) {
        return Math.abs(dateTemperaturePair.getYearMonth().hashCode() % numberOfPartitions);
    }
}
