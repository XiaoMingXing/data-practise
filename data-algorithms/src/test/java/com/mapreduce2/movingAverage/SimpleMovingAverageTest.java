package com.mapreduce2.movingAverage;

import org.junit.jupiter.api.Test;

import static com.google.common.collect.Lists.newArrayList;

public class SimpleMovingAverageTest {

    @Test
    void shouldPrintCorrectInfo() {
        newArrayList(3, 4).forEach(windowSize -> {
            System.out.println("windowSize:" + windowSize);
            SimpleMovingAverageUsingArray movingAverage = new SimpleMovingAverageUsingArray(windowSize);
            newArrayList(10, 18, 20, 30, 24, 33, 27).forEach(x -> {
                movingAverage.addNewNumber(x);
                System.out.println(String
                        .format("Next number= %s, SMA= %s", x, movingAverage.getMovingAverage()));
            });
        });

    }
}