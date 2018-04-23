package com.mapreduce2.movingAverage;


import com.common.DateUtil;
import io.netty.util.internal.StringUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class SimpleMovingAverageMapper extends Mapper<LongWritable, Text, CompositeKey, TimeSeriesData> {

    private final CompositeKey mapperKey = new CompositeKey();
    private final TimeSeriesData mapperValue = new TimeSeriesData();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String record = value.toString();
        if (StringUtil.isNullOrEmpty(record)) {
            return;
        }
        String[] tokens = StringUtils.split(record, ',');
        if (tokens.length < 3) {
            return;
        }
        long timestamp = DateUtil.getTimestamp(tokens[1]);
        mapperKey.setName(tokens[0]);
        mapperKey.setTimestamp(timestamp);
        mapperValue.setTimestamp(timestamp);
        mapperValue.setValue(Double.parseDouble(tokens[2]));
        context.write(mapperKey, mapperValue);
    }
}
