package com.mapreduce2.movingAverage;

import java.util.LinkedList;
import java.util.Queue;

public class SimpleMovingAverage {

    private double sum = 0.0;

    private final int period;

    private final Queue<Double> window = new LinkedList<>();

    public SimpleMovingAverage(int period) {
        if (period < 1) {
            throw new IllegalArgumentException("period must be > 0");
        }
        this.period = period;
    }

    public void addNewNumber(double number) {
        sum += number;
        window.add(number);
        if (window.size() > this.period) {
            sum -= window.remove();
        }
    }

    public double getMovingAverage() {
        if (window.isEmpty()) {
            throw new IllegalArgumentException("average is undefined");
        }
        return sum / window.size();
    }
}
