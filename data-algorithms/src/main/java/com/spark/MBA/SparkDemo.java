package com.spark.MBA;

import org.apache.spark.SparkConf;

public class SparkDemo {

    private static void main(String[] args){
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: " + " <input-path> <output-path>");
        }

        new SparkConf();

    }
}
