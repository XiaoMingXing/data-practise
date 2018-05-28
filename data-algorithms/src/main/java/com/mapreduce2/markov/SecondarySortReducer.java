package com.mapreduce2.markov;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecondarySortReducer extends Reducer<CompositeKey, Value, Text, Text> {

    @Override
    public void reduce(CompositeKey key, Iterable<Value> values, Context context)
            throws IOException, InterruptedException {
        StringBuffer stringBuffer = new StringBuffer();
        values.forEach(value -> stringBuffer.append(",").append(value.getPurchaseDate()));
        context.write(new Text(key.getCustomerId()), new Text(stringBuffer.toString()));
    }
}
