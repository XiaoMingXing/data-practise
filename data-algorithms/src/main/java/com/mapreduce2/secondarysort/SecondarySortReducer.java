package com.mapreduce2.secondarysort;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class SecondarySortReducer extends Reducer<DateTemperaturePair, Text, Text, Text> {

    private final Log logger = LogFactory.getLog(SecondarySortReducer.class);


    @Override
    protected void reduce(DateTemperaturePair key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        StringBuilder builder = new StringBuilder();


        for (Text value : values) {
            builder.append(value.toString());
            builder.append(",");
        }
        logger.info(String.format("THE VALUES IN REDUCER: %s", builder.toString()));
        context.write(key.getYearMonth(), new Text(builder.toString()));
    }
}
