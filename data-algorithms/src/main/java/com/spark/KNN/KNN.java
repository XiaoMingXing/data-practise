package com.spark.KNN;

import com.spark.SparkContextBuilder;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static org.apache.curator.shaded.com.google.common.collect.Lists.newArrayList;

public class KNN {


    // todo add frameworks to calculate the algorithms running time
    // todo add data generator to generate testing / training data
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

        JavaPairRDD<String, String> res = classificationByCombine(broadcastD, knnMapped);
        classificationByGroup(broadcastK, knnMapped);
        res.collect().forEach(System.out::println);
    }

    private static void classificationByGroup(Broadcast<Integer> broadcastK, JavaPairRDD<String, Tuple2<Double, String>> knnMapped) {
        JavaPairRDD<String, String> result = knnMapped.groupByKey()
                .mapValues((Function<Iterable<Tuple2<Double, String>>, String>) neighbors -> {
                    SortedMap<Double, String> nearestK = KNNHelper.findNearestK(neighbors,
                            broadcastK.value());
                    Map<String, Integer> majority = KNNHelper.buildClassificationCount(nearestK);
                    return KNNHelper.classifyByMajority(majority);
                });
    }

    private static JavaPairRDD<String, String> classificationByCombine(Broadcast<Integer> broadcastD, JavaPairRDD<String, Tuple2<Double, String>> knnMapped) {

        // use the combineByKey to find the nearestK

        // for the first <distance,classificationID>, how we get the <result>
        // for new <distance,classificationID> pair comes in, how we merge into <result> -- partition level
        // for two <Result> how we merge into one result -- cluster level
        return knnMapped.combineByKey(
                // create combiner (distance,classificationID)->classificationID
                (Function<Tuple2<Double, String>, List<Tuple2<Double, String>>>) Lists::newArrayList,
                // merge value
                (Function2<List<Tuple2<Double, String>>, Tuple2<Double, String>,
                        List<Tuple2<Double, String>>>) (neighbors, current) -> {
                    neighbors.add(current);
                    SortedMap<Double, String> nearestK = KNNHelper.findNearestK(neighbors, broadcastD.value());
                    return mapToList(nearestK);
                },
                (Function2<List<Tuple2<Double, String>>, List<Tuple2<Double, String>>,
                        List<Tuple2<Double, String>>>) (v1, v2) -> {
                    v1.addAll(v2);
                    SortedMap<Double, String> nearestK = KNNHelper.findNearestK(v1, broadcastD.getValue());
                    return mapToList(nearestK);
                }
        ).mapValues((nearestK) -> {
            Map<String, Integer> majority = KNNHelper.buildClassificationCountInMap(nearestK);
            return KNNHelper.classifyByMajority(majority);
        });
    }

    private static List<Tuple2<Double, String>> mapToList(SortedMap<Double, String> nearestK) {
        List<Tuple2<Double, String>> partitionNeighbors = newArrayList();
        nearestK.forEach((distance, classificationID) ->
                partitionNeighbors.add(new Tuple2<>(distance, classificationID)));
        return partitionNeighbors;
    }
}
