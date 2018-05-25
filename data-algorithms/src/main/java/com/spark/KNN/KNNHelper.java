package com.spark.KNN;

import com.google.common.base.Splitter;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;

class KNNHelper {

    private static final String DELIMITER = ",";


    private static List<Double> splitOnToListOfDouble(String str) {
        return newArrayList(Splitter.on(KNNHelper.DELIMITER).trimResults().split(str))
                .stream()
                .map(Double::parseDouble)
                .collect(Collectors.toList());
    }

    static Double calculateDistance(String rAsString, String sAsString, int dimensions) {
        List<Double> pointA = splitOnToListOfDouble(rAsString);
        List<Double> pointB = splitOnToListOfDouble(sAsString);

        if (pointA.size() != dimensions || pointB.size() != dimensions) {
            return Double.NaN;
        }

        return euclideanDistance(dimensions, pointA, pointB);
    }

    private static Double euclideanDistance(int dimensions, List<Double> pointA, List<Double> pointB) {
        double sum = 0d;
        for (int index = 0; index < dimensions; index++) {
            double difference = pointA.get(index) - pointB.get(index);
            sum += difference * difference;
        }
        return Math.sqrt(sum);
    }

    static SortedMap<Double, String> findNearestK(Iterable<Tuple2<Double, String>> neighbors, int k) {

        TreeMap<Double, String> nearestK = new TreeMap<>();
        neighbors.forEach(neighbor -> {
            Double distance = neighbor._1;
            String classificationID = neighbor._2;
            nearestK.put(distance, classificationID);
            if (nearestK.size() > k) {
                nearestK.remove(nearestK.lastKey());
            }
        });
        return nearestK;
    }


    static Map<String, Integer> buildClassificationCount(Map<Double, String> nearestK) {
        Map<String, Integer> majority = new HashMap<>();
        nearestK.forEach((key, classificationID) ->
                majority.merge(classificationID, 1, (exist, current) -> exist + current));
        return majority;
    }

    static String classifyByMajority(Map<String, Integer> majority) {
        return Collections.max(majority.entrySet(),
                Comparator.comparing(Map.Entry::getValue)).getKey();
    }
}
