package com.mapreduce2.movingAverage;

import com.common.DateUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SimpleMovingAverageReducer extends Reducer<CompositeKey, TimeSeriesData, Text, Text> {

    private int windowSize = 5;
    private final Text outputKey = new Text();
    private final Text outputValue = new Text();

    @Override
    public void setup(Context context) {
        this.windowSize = context.getConfiguration().getInt("moving.average.window.size", 5);
    }

    @Override
    public void reduce(CompositeKey key, Iterable<TimeSeriesData> values, Context context)
            throws IOException, InterruptedException {
        SimpleMovingAverage simpleMovingAverage = new SimpleMovingAverage(this.windowSize);
        for (TimeSeriesData value : values) {
            simpleMovingAverage.addNewNumber(value.getValue());
            double average = simpleMovingAverage.getMovingAverage();
            String dateStr = DateUtil.getDateAsString(value.getTimestamp());
            outputKey.set(key.getName());
            outputValue.set(String.format("%s , %s", dateStr, average));
            context.write(outputKey, outputValue);
        }
    }


}
