package com.spark.markov;

import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SparkMarkovTest {

    @Test
    public void shouldReturnNullIfNoValues() {
        List<String> list = SparkMarkov.toStateSequence(null);
        assertThat(list, is(nullValue()));
    }

    @Test
    public void shouldReturnStates() {
        String date1 = "2017-08-19";
        String date2 = "2017-08-20";
        String date3 = "2017-12-21";
        ArrayList<Tuple2<String, Integer>> values = newArrayList(
                new Tuple2<>(date1, 160),
                new Tuple2<>(date2, 200),
                new Tuple2<>(date3, 400));
        List<String> result = SparkMarkov.toStateSequence(values);
        checkNotNull(result);
        assertThat(result.size(), is(2));
        assertThat(result.get(0), is("SL"));
        assertThat(result.get(1), is("LL"));
    }
}