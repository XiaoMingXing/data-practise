package com.spark.MBA;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;

public class MBAnalysis {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: " + " <input-path>");
        }
        String inputPath = args[0];
        JavaSparkContext jsc = MarketBasketAnalysis.createJavaSparkContext();

        JavaRDD<String> javaRDD = jsc.textFile(inputPath);
        JavaRDD<List<String>> lines = javaRDD.map(line ->
                Arrays.stream(line.split(","))
                        .map(String::trim)
                        .collect(Collectors.toList()));
        List<String> header = lines.first();
        JavaRDD<List<String>> rows = lines.filter((line) -> !line.get(0).equals(header.get(0)));

        JavaRDD<List<String>> transactions = rows
                .mapToPair(row -> new Tuple2<>(row.get(0), row.get(2)))
                .groupByKey()
                .mapValues(values -> newArrayList(values).stream()
                        .map(MBAnalysis::toProductName).collect(Collectors.toList()))
                .values();

        System.out.println("TRANSACTIONS: " + transactions.count());
    }

    private static String toProductName(String description) {

        description.replaceAll("(PINK|RED)", "");

        return description;
    }

}
