package com.spark.secondarysort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;

import static com.google.common.collect.Lists.newArrayList;

public class SecondarySort {

    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("Usage: SecondarySort <file>");
            System.exit(1);
        }
        String inputPath = args[0];
        System.out.println("inputPath=" + inputPath);

        String outputPath = args[1];
        System.out.println("outputPath=" + outputPath);

        // connect to the spark master
        SparkConf conf = new SparkConf()
                .setAppName("SecondarySort")
                .setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> values = ctx
                .textFile(inputPath, 1)
                .mapToPair((PairFunction<String, String, Tuple2<Integer, Integer>>) s -> {
                    String[] tokens = s.split(",");
                    System.out.println(tokens[0] + "," + tokens[1] + "," + tokens[2]);
                    Integer time = new Integer(tokens[1]);
                    Integer value = new Integer(tokens[2]);
                    Tuple2<Integer, Integer> timeValue = new Tuple2<>(time, value);
                    return new Tuple2<>(tokens[0], timeValue);
                })
                .groupByKey()
                .mapValues((Function<Iterable<Tuple2<Integer, Integer>>,
                        Iterable<Tuple2<Integer, Integer>>>) v1 -> {
                    ArrayList<Tuple2<Integer, Integer>> list = newArrayList(v1);
                    Collections.sort(list, new TupleComparator());
                    return list;
                });

        values.collect().forEach((t) -> {
            Iterable<Tuple2<Integer, Integer>> list = t._2;
            System.out.println(t._1);
            for (Tuple2<Integer, Integer> t2 : list) {
                System.out.println(t2._1 + "," + t2._2);
            }
            System.out.println("=====");
        });
    }
}
