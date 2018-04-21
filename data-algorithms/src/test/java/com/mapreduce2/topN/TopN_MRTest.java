package com.mapreduce2.topN;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class TopN_MRTest {

    MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
    ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;

    @Before
    public void setUp() {
        mapDriver = MapDriver.newMapDriver(new TopN_Mapper());
        reduceDriver = ReduceDriver.newReduceDriver(new TopN_Reducer());
    }

    @Test
    public void shouldReturnSuccess() throws Exception {
        mapDriver.withInput(new LongWritable(1), new Text("15,cat15,cat15"));
        mapDriver.withInput(new LongWritable(1), new Text("12,cat12,cat12"));
        mapDriver.withInput(new LongWritable(1), new Text("9,cat9,cat9"));
        mapDriver.withInput(new LongWritable(1), new Text("19,cat19,cat19"));
        mapDriver.withInput(new LongWritable(1), new Text("22,cat14,cat19"));
        mapDriver.withInput(new LongWritable(1), new Text("5,cat5,cat5"));

        List<Pair<Text, DoubleWritable>> pairs = mapDriver.run();

        pairs.forEach(pair -> System.out.println(pair.toString()));
    }

    @Test
    public void shouldReduceTopNMethods() throws Exception {
        reduceDriver.withInput(new Text("cat9"),
                newArrayList(new DoubleWritable(9), new DoubleWritable(10)));

        reduceDriver.withInput(new Text("cat5"),
                newArrayList(new DoubleWritable(4), new DoubleWritable(8)));

        reduceDriver.withInput(new Text("cat10"),
                newArrayList(new DoubleWritable(3), new DoubleWritable(18)));
        reduceDriver.withInput(new Text("cat100"),
                newArrayList(new DoubleWritable(100), new DoubleWritable(88)));
        reduceDriver.withInput(new Text("cat31"),
                newArrayList(new DoubleWritable(31), new DoubleWritable(89)));

        List<Pair<Text, DoubleWritable>> pairs = reduceDriver.run();
        pairs.forEach(pair -> System.out.println(pair.toString()));
    }
}