package com.mapreduce2.OrderInversion;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


public class OrderInversionMRTest {

    MapDriver<LongWritable, Text, PairOfWords, IntWritable> mapDriver;
    ReduceDriver<PairOfWords, IntWritable, PairOfWords, DoubleWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, PairOfWords, IntWritable, PairOfWords, DoubleWritable> mapReduceDriver;

    @Before
    public void setUp() {
        RelativeFrequencyMapper mapper = new RelativeFrequencyMapper();
        RelativeFrequencyReducer reducer = new RelativeFrequencyReducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }


    @Test
    public void shouldReturn9Pairs() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "I love eat"));
        List<Pair<PairOfWords, IntWritable>> pairs = mapDriver.run();
        assertThat(pairs.size(), is(9));
    }

    @Test
    public void shouldReturn1Pairs() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "I"));
        List<Pair<PairOfWords, IntWritable>> pairs = mapDriver.run();
        assertThat(pairs.size(), is(0));
    }

    @Test
    public void shouldCombineTheValuesInMapStage() throws IOException {
        ReduceDriver<PairOfWords, IntWritable, PairOfWords, IntWritable> reduceDriver =
                ReduceDriver.newReduceDriver(new RelativeFrequencyCombiner());
        ;
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(2));
        PairOfWords pairWord = new PairOfWords("I", "love");
        reduceDriver.withInput(pairWord, values);
        List<Pair<PairOfWords, IntWritable>> pairs = reduceDriver.run();

        assertThat(pairs.size(), is(1));
        assertThat(pairs.get(0).getSecond().get(), is(4));
        assertThat(pairs.get(0).getFirst().getLeftElement(), is("I"));
    }

    @Test
    public void shouldReduce() throws Exception {

        // should make sure the order
        PairOfWords pairWord3 = new PairOfWords("I", "*");
        reduceDriver.withInput(pairWord3, newArrayList(new IntWritable(7), new IntWritable(8)));
        PairOfWords pairWord = new PairOfWords("I", "love");
        reduceDriver.withInput(pairWord, newArrayList(new IntWritable(5), new IntWritable(5)));
        PairOfWords pairWord2 = new PairOfWords("I", "guess");
        reduceDriver.withInput(pairWord2, newArrayList(new IntWritable(2), new IntWritable(3)));

        PairOfWords pairWordC = new PairOfWords("love", "*");
        reduceDriver.withInput(pairWordC, newArrayList(new IntWritable(5), new IntWritable(5)));
        PairOfWords pairWordA = new PairOfWords("love", "I");
        reduceDriver.withInput(pairWordA, newArrayList(new IntWritable(2), new IntWritable(3)));
        PairOfWords pairWordB = new PairOfWords("love", "coding");
        reduceDriver.withInput(pairWordB, newArrayList(new IntWritable(3), new IntWritable(2)));


        List<Pair<PairOfWords, DoubleWritable>> pairs = reduceDriver.run();
        assertThat(pairs.size(), is(4));

        pairs.forEach((pair) -> {
            String word = pair.getFirst().getLeftElement();
            String ref = pair.getFirst().getRightElement();
            double value = pair.getSecond().get();
            System.out.println(String.format("(%s,%s):%s", word, ref, value));
        });

    }
}