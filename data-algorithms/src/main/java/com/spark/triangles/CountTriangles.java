package com.spark.triangles;

import com.spark.SparkContextBuilder;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Comparator;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class CountTriangles {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: CountTriangles " + " <hdfs file>");
        }

        String inputPath = args[0];
        JavaSparkContext jsc = SparkContextBuilder.createJavaSparkContext();

        jsc.textFile(inputPath)
                .flatMapToPair((record) -> {
                    String[] nodes = record.split(",");
                    String start = nodes[0];
                    String end = nodes[1];
                    List<Tuple2<String, String>> result =
                            newArrayList(new Tuple2<>(start, end), new Tuple2<>(end, start));
                    return result.iterator();
                })
                .groupByKey()
                .flatMapToPair((record) -> {
                    String start = record._1;
                    List<String> ends = newArrayList(record._2);
                    ends.sort(Comparator.naturalOrder());

                    List<Tuple2<Tuple2<String, String>, String>> result = newArrayList();

                    // generate the <<start,end> ,"-"> which means start is connected
                    ends.forEach(end -> {
                        result.add(new Tuple2<>(new Tuple2<>(start, end), "-"));
                    });
                    // generate the <<end1,end2> ,start>, <<end1,end3> ,start> ... <<endN,endM> ,start>
                    // which means endN and endM are all connected to the start

                    int endsSize = ends.size();
                    for (int index = 0; index < endsSize - 1; index++) {
                        String endA = ends.get(index);
                        String endB = ends.get(index + 1);
                        result.add(new Tuple2<>(new Tuple2<>(endA, endB), start));
                    }
                    return result.iterator();
                })
                .groupByKey()
                .filter(record -> newArrayList(record._2).size() > 1)
                .flatMap(record -> {
                    Tuple2<String, String> key = record._1;
                    List<Tuple3<String, String, String>> result = newArrayList();
                    record._2.forEach(node -> {
                        if (!node.equals("-")) {
                            result.add(new Tuple3<>(key._1, key._2, node));
                        }
                    });
                    return result.iterator();
                })
                .map(record -> {
                    List<String> result = newArrayList(record._1(), record._2(), record._3());
                    result.sort(Comparator.naturalOrder());
                    return result;
                })
                .distinct()
                .collect().forEach(System.out::println);
    }

}
