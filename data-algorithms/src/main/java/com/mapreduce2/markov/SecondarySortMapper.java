package com.mapreduce2.markov;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondarySortMapper extends Mapper<LongWritable, Text, CompositeKey, Value> {

    // 301UNH7I2F,1381872899,2013-01-01,148 -> (301UNH7I2F,2013-01-01)->(2013-01-01,148)
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if (value == null) {
            return;
        }
        String[] parts = value.toString().split(",");
        String customerId = parts[0];
        String date = parts[2];
        long amount = Long.parseLong(parts[3]);

        CompositeKey k = new CompositeKey(customerId, date);
        Value v = new Value(date, amount);
        context.write(k, v);
    }

}
