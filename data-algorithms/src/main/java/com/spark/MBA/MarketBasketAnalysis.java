package com.spark.MBA;

import com.common.Combination;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class MarketBasketAnalysis {

    private static JavaSparkContext createJavaSparkContext() throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("market basket analysis")
                .setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryoserializer.buffer.mb", "32");
        return new JavaSparkContext(conf);
    }

    private static List<String> toList(String transaction) {
        return newArrayList(transaction.trim().split(","));
    }

    static List<String> removeOneItem(List<String> list, int index) {
        if (CollectionUtils.isEmpty(list)) {
            return list;
        }
        if ((index < 0) || (index > (list.size() - 1))) {
            return list;
        }
        List<String> clone = newArrayList(list);
        clone.remove(index);
        return clone;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("Usage: FindAssociationRules" + " <transactions>");
        }
        String transactionsFileName = args[0];
        createJavaSparkContext()
                .textFile(transactionsFileName, 1)
                .flatMapToPair(transaction -> {
                    List<Tuple2<List<String>, Integer>> result = newArrayList();
                    Combination.findSortedCombinations(toList(transaction))
                            .forEach((combination) -> {
                                if (combination.size() > 0) {
                                    result.add(new Tuple2<>(combination, 1));
                                }
                            });
                    return result.iterator();
                })
                .reduceByKey((first, second) -> first + second)
                .flatMapToPair((PairFlatMapFunction<Tuple2<List<String>, Integer>,
                        List<String>, Tuple2<List<String>, Integer>>) pattern -> {
                    List<Tuple2<List<String>, Tuple2<List<String>, Integer>>> result = newArrayList();
                    List<String> list = pattern._1;
                    Integer frequency = pattern._2;
                    result.add(new Tuple2<>(list, new Tuple2<>(null, frequency)));
                    if (list.size() == 1) {
                        return result.iterator();
                    }
                    for (int index = 0; index < list.size(); index++) {
                        List<String> sublist = removeOneItem(list, index);
                        result.add(new Tuple2<>(sublist, new Tuple2<>(list, frequency)));
                    }

                    return result.iterator();
                })
                .groupByKey()
                .map((in) -> {
                    List<Tuple3<List<String>, List<String>, Double>> result = newArrayList();
                    List<String> fromList = in._1;
                    Iterable<Tuple2<List<String>, Integer>> to = in._2;
                    List<Tuple2<List<String>, Integer>> toList = newArrayList();
                    Tuple2<List<String>, Integer> fromCount = null;
                    for (Tuple2<List<String>, Integer> t2 : to) {
                        if (t2._1 == null) {
                            fromCount = t2;
                        } else {
                            toList.add(t2);
                        }
                    }
                    if (toList.isEmpty()) {
                        return result;
                    }
                    for (Tuple2<List<String>, Integer> t2 : toList) {
                        double confidence = (double) t2._2 / (double) fromCount._2;
                        ArrayList<String> t2List = newArrayList(t2._1);
                        t2List.removeAll(fromList);
                        result.add(new Tuple3<>(fromList, t2List, confidence));
                    }
                    return result;
                })
                .filter((list) -> list.size() != 0)
                .saveAsTextFile("rules/output/6");


    }

}
