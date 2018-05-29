package com.spark.markov;

import com.google.common.collect.Lists;
import com.spark.SparkContextBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;

public class SparkMarkov {


    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Spark Markov Usage: " + " <input-path>");
        }

        final String inputPath = args[0];

        JavaSparkContext jsc = SparkContextBuilder.createJavaSparkContext();
        JavaRDD<String> records = jsc.textFile(inputPath);

        List<Tuple2<Tuple2<String, String>, Integer>> stateTransformList = records
                .mapToPair((record) -> {
                    String[] tokens = StringUtils.split(record, ',');
                    if (tokens.length != 4) {
                        return null;
                    }
                    int amount = Integer.parseInt(tokens[3]);
                    Tuple2<String, Integer> value = new Tuple2<>(tokens[2], amount);
                    return new Tuple2<>(tokens[0], value);
                })
                .groupByKey()
                .mapValues((values) -> {
                    List<Tuple2<String, Integer>> list = newArrayList(values);
                    list.sort((o1, o2) -> o1._1.compareTo(o2._1));
                    return toStateSequence(list);
                })
                .flatMapToPair((record) -> {
                    List<Tuple2<Tuple2<String, String>, Integer>> output = newArrayList();
                    List<String> states = record._2;
                    if (CollectionUtils.isEmpty(states) || states.size() < 2) {
                        return output.iterator();
                    }
                    for (int index = 0; index < states.size() - 1; index++) {
                        String fromState = states.get(index);
                        String toState = states.get(index + 1);
                        output.add(new Tuple2<>(new Tuple2<>(fromState, toState), 1));
                    }
                    return output.iterator();
                })
                .combineByKey(
                        v1 -> v1,
                        (v1, v2) -> v1 + v2,
                        (v1, v2) -> v1 + v2)
                .collect();

        List<Tuple2<String, List<Tuple2<String, Double>>>> result = normalize(stateTransformList);
        result.forEach(System.out::println);
    }

    private static List<Tuple2<String, List<Tuple2<String, Double>>>>
    normalize(List<Tuple2<Tuple2<String, String>, Integer>> list) {

        List<String> states = newArrayList("SL", "SE", "SG", "ML", "ME", "MG", "LL", "LE", "LG");
        return states.stream()
                .map((state) -> {
                    List<Tuple2<String, Long>> transformTo = findTransformToStates(state, list);
                    long totalAmount = getTotalAmount(transformTo);
                    List<Tuple2<String, Double>> transformList = newArrayList();
                    transformTo.forEach((item) -> {
                        transformList.add(new Tuple2<>(item._1, (double) item._2 / totalAmount));
                    });
                    return new Tuple2<>(state, transformList);
                })
                .collect(Collectors.toList());
    }

    private static long getTotalAmount(List<Tuple2<String, Long>> transformTo) {
        long total = 0;
        for (Tuple2<String, Long> record : transformTo) {
            total += record._2();
        }
        return total;
    }

    private static List<Tuple2<String, Long>> findTransformToStates(String state,
                                                                    List<Tuple2<Tuple2<String, String>, Integer>> list) {
        return list.stream()
                .filter((record) -> state.equals(record._1()._1))
                .map(record -> new Tuple2<>(record._1()._2, (long) record._2))
                .collect(Collectors.toList());
    }

    static List<String> toStateSequence(List<Tuple2<String, Integer>> values) {
        // Based on the time between previous and current, we have three category (S, M, L) short middle long
        // Based on the amount between previous and current, we have three categories (L, E, G) - less, equal, great
        if (CollectionUtils.isEmpty(values)) {
            return null;
        }
        ArrayList<String> list = Lists.newArrayList();
        for (int index = 1; index < values.size(); index++) {
            Tuple2<String, Integer> previous = values.get(index - 1);
            Tuple2<String, Integer> current = values.get(index);
            String durationLevel = getDurationLevel(previous, current);
            String amountLevel = getAmountLevel(previous, current);
            list.add(durationLevel + amountLevel);
        }
        return list;
    }

    private static String getAmountLevel(Tuple2<String, Integer> previous, Tuple2<String, Integer> current) {
        int previousAmount = previous._2();
        int currentAmount = current._2();
        if (previousAmount < 0.9 * currentAmount) {
            return "L";
        } else if (previousAmount < 1.1 * currentAmount) {
            return "E";
        } else {
            return "G";
        }
    }

    private static String getDurationLevel(Tuple2<String, Integer> previous, Tuple2<String, Integer> current) {
        String previousTime = previous._1();
        String currentTime = current._1();

        long daysDiff = ChronoUnit.DAYS.between(
                LocalDate.parse(previousTime),
                LocalDate.parse(currentTime));
        if (daysDiff < 30) {
            return "S";
        } else if (daysDiff < 60) {
            return "M";
        } else {
            return "L";
        }
    }


}
