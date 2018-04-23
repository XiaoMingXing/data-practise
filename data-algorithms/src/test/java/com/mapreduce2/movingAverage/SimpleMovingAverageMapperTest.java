package com.mapreduce2.movingAverage;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class SimpleMovingAverageMapperTest {

    MapDriver<LongWritable, Text, CompositeKey, TimeSeriesData> mapDriver;
    ReduceDriver<CompositeKey, TimeSeriesData, Text, Text> reduceDriver;

    @Before
    public void setUp() {
        mapDriver = MapDriver.newMapDriver(new SimpleMovingAverageMapper());
        reduceDriver = ReduceDriver.newReduceDriver(new SimpleMovingAverageReducer());
    }

    @Test
    public void shouldReturnSuccess() throws Exception {

        mapDriver.withInput(new LongWritable(1), new Text("GOOG,2004-11-04,184.70"));
        mapDriver.withInput(new LongWritable(2), new Text("GOOG,2004-11-03,191.67"));
        mapDriver.withInput(new LongWritable(3), new Text("GOOG,2004-11-02,194.87"));
        mapDriver.withInput(new LongWritable(4), new Text("AAPL,2013-10-09,486.59"));
        mapDriver.withInput(new LongWritable(5), new Text("AAPL,2013-10-08,480.94"));
        List<Pair<CompositeKey, TimeSeriesData>> pairs = mapDriver.run();
        pairs.forEach(pair -> System.out.println(String
                .format("Name:%s, Timestamp %s: Timestamp: %s , Value: %s",
                        pair.getFirst().getName(), pair.getFirst().getTimestamp()
                        , pair.getSecond().getTimestamp(), pair.getSecond().getValue())));
    }


    @Test
    public void shouldTestReducerClass() throws Exception {
        CompositeKey key = new CompositeKey("GOOL", 1073146260000L);
        TimeSeriesData value = new TimeSeriesData(1073146260000L, 100d);

        CompositeKey key2 = new CompositeKey("GOOL", 1073059860000L);
        TimeSeriesData value2 = new TimeSeriesData(1073059860000L, 150d);

        CompositeKey key3 = new CompositeKey("GOOL", 1357661400000L);
        TimeSeriesData value3 = new TimeSeriesData(1357661400000L, 200d);

        reduceDriver.withInput(key, newArrayList(value));
        reduceDriver.withInput(key2, newArrayList(value2));
        reduceDriver.withInput(key3, newArrayList(value3));

        List<Pair<Text, Text>> pairs = reduceDriver.run();

        pairs.forEach(textTextPair -> System.out.println(textTextPair.toString()));
    }
}