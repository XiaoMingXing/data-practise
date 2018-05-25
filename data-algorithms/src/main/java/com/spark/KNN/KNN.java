package com.spark.KNN;

import com.spark.SparkContextBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;
import java.util.SortedMap;

public class KNN {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException("KNN Usage: " + " <input-path> <output-path>");
        }

        Integer k = Integer.valueOf(args[0]);
        Integer d = Integer.valueOf(args[1]);

        String datasetR = args[2];
        String datasetS = args[3];

        JavaSparkContext jsc = SparkContextBuilder.createJavaSparkContext();

        final Broadcast<Integer> broadcastK = jsc.broadcast(k);
        final Broadcast<Integer> broadcastD = jsc.broadcast(d);

        JavaRDD<String> R = jsc.textFile(datasetR);
        JavaRDD<String> S = jsc.textFile(datasetS);

        JavaPairRDD<String, String> cart = R.cartesian(S);

        JavaPairRDD<String, Tuple2<Double, String>> knnMapped =
                cart.mapToPair((PairFunction<Tuple2<String, String>, String, Tuple2<Double, String>>)
                        cardRecord -> {
                            String rRecord = cardRecord._1;
                            String sRecord = cardRecord._2;

                            String[] rTokens = rRecord.split(";");
                            String rRecordID = rTokens[0];
                            String r = rTokens[1];

                            String[] sTokens = sRecord.split(";");
                            String sClassificationID = sTokens[1];
                            String s = sTokens[2];

                            Integer dimensions = broadcastD.value();
                            double distance = KNNHelper.calculateDistance(r, s, dimensions);

                            Tuple2<Double, String> V = new Tuple2<>(distance, sClassificationID);
                            return new Tuple2<>(rRecordID, V);
                        });

        JavaPairRDD<String, String> result = knnMapped.groupByKey()
                .mapValues((Function<Iterable<Tuple2<Double, String>>, String>) neighbors -> {
                    SortedMap<Double, String> nearestK = KNNHelper.findNearestK(neighbors,
                            broadcastK.value());
                    Map<String, Integer> majority = KNNHelper.buildClassificationCount(nearestK);
                    return KNNHelper.classifyByMajority(majority);
                });

        result.collect().forEach(System.out::println);
    }
}
