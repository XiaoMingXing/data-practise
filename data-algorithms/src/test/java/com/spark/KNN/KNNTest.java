package com.spark.KNN;

import org.junit.Test;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class KNNTest {

    @Test
    public void classifyByMajority() {

        Map<String, Integer> majority = newHashMap();
        majority.put("C1", 1);
        majority.put("C2", 1);
        majority.put("C3", 1);
        String classify = KNNHelper.classifyByMajority(majority);
        assertThat(classify, is("C3"));
    }
}