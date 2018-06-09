package com.spark.bayes;

import com.spark.SparkContextBuilder;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BuildNaiveBayesClassifier {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Naive Bayes Usage: " + " <input-path> <output-path>");
        }

        String inputFilePath = args[0];

        JavaSparkContext jsc = SparkContextBuilder.createJavaSparkContext();


        JavaRDD<String> javaRDD = jsc.textFile(inputFilePath);
        long trainingDataSize = javaRDD.count();
        Map<Tuple2<String, String>, Integer> countsAsMap = javaRDD.flatMapToPair((PairFlatMapFunction<String, Tuple2<String, String>, Integer>) record -> {
            List<Tuple2<Tuple2<String, String>, Integer>> tuples = new ArrayList<>();
            String[] tokens = record.split(",");
            int classificationIndex = tokens.length - 1;
            String theClassification = tokens[classificationIndex];

            for (int index = 1; index < classificationIndex - 1; index++) {
                Tuple2<String, String> K = new Tuple2<>(tokens[index], theClassification);
                tuples.add(new Tuple2<>(K, 1));
            }
            tuples.add(new Tuple2<>(new Tuple2<String, String>("CLASS", theClassification), 1));
            return tuples.iterator();
        })
                .reduceByKey((prev, current) -> prev + current)
                .collectAsMap();

//        countsAsMap.forEach((k, v) -> System.out.println(k + ":" + v));

        HashMap<Tuple2<String, String>, Double> PT = new HashMap<>();
        List<String> CLASSIFICATIONS = new ArrayList<>();

        countsAsMap.forEach((key, value) -> {
            String classification = key._2;
            if (key._1.equals("CLASS")) {
                PT.put(key, (double) value / trainingDataSize);
                CLASSIFICATIONS.add(classification);
            } else {
                Tuple2<String, String> k2 = new Tuple2<>("CLASS", classification);
                Integer count = countsAsMap.get(k2);
                if (count == 0) {
                    PT.put(key, 0.0);
                } else {
                    PT.put(key, (double) value / count);
                }
            }
        });

        PT.forEach((key, value) -> System.out.println(key + ":" + value));
        System.out.println(CLASSIFICATIONS);

    }

}
