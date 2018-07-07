package com.spark.triangles;

import scala.Tuple2;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class TriangleHelper {

    public static List<Tuple2<String, String>> mapToEdges(String record) {
        String[] nodes = record.split(",");
        String start = nodes[0];
        String end = nodes[1];
        return newArrayList(new Tuple2<>(start, end), new Tuple2<>(end, start));
    }
}
