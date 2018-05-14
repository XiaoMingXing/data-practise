package com.spark.recommendation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple7;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.spark.SparkContextBuilder.createJavaSparkContext;

public class MovieRecommendationWithJoin {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: " + " <input-path> <output-path>");
        }

        String inputPath = args[0];
        String outputPath = args[1];

        JavaSparkContext jsc = createJavaSparkContext();
        JavaRDD<String> userRatings = jsc.textFile(inputPath);

        JavaPairRDD<String, Tuple3<String, Integer, Integer>> usersRDD = calculateNumberOfRaters(userRatings);

        JavaPairRDD<String, Tuple2<Tuple3<String, Integer, Integer>,
                Tuple3<String, Integer, Integer>>> joinRDD = joinRDD(usersRDD);


        JavaPairRDD<Tuple2<String, String>,
                Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>>
                rddWithValue = calculateRecordValue(joinRDD);

        // collect the scores of (movieA, movieB) from different users
        JavaPairRDD<Tuple2<String, String>, Tuple3<Double, Double, Double>> results = rddWithValue
                .groupByKey()
                .mapValues(MovieRecommendationWithJoin::calculateCorrelations);

        results.saveAsTextFile(outputPath);
    }

    public static JavaPairRDD<Tuple2<String, String>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> calculateRecordValue(JavaPairRDD<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>> joinRDD) {
        return joinRDD.mapToPair(item -> {
            Tuple3<String, Integer, Integer> movie1 = item._2._1;
            Tuple3<String, Integer, Integer> movie2 = item._2._2;
            Tuple2<String, String> resultKey = new Tuple2<>(movie1._1(), movie2._1());

            // don't know what's this
            int ratingProduct = movie1._2() * movie2._2();

            // calculate x^2 and y^2
            int rating1Squared = movie1._2() * movie1._2();
            int rating2Squared = movie2._2() * movie2._2();

            Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> resultValue = new Tuple7<>(
                    movie1._2(), // movie1.rating as x
                    movie1._3(), // movie1.numberOfRaters n(x)
                    movie2._2(), // movie2.rating as y
                    movie2._3(), // movie2.numberOfRaters n(y)
                    ratingProduct, // x*y
                    rating1Squared, // x^2
                    rating2Squared  // y^2
            );
            return new Tuple2<>(resultKey, resultValue);
        });
    }

    public static JavaPairRDD<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>> joinRDD(JavaPairRDD<String, Tuple3<String, Integer, Integer>> usersRDD) {
        return usersRDD
                .join(usersRDD)
                .filter(item -> {
                    Tuple3<String, Integer, Integer> movie1 = item._2._1;
                    Tuple3<String, Integer, Integer> movie2 = item._2._2;
                    String movieName1 = movie1._1();
                    String movieName2 = movie2._1();
                    if (movieName1.compareTo(movieName2) < 0) {
                        return true;
                    }
                    return false;
                });
    }

    public static JavaPairRDD<String, Tuple3<String, Integer, Integer>> calculateNumberOfRaters(JavaRDD<String> userRatings) {
        return userRatings
                .mapToPair((s) -> {
                    String[] record = s.split("\\s");
                    String user = record[0];
                    String movie = record[1];
                    Integer score = Integer.parseInt(record[2]);
                    return new Tuple2<>(movie, new Tuple2<>(user, score));
                })
                .groupByKey()
                .flatMapToPair(pair -> {
                    String movie = pair._1;
                    Iterable<Tuple2<String, Integer>> pairsOfUserAndRating = pair._2;
                    int numberOfRaters = newArrayList(pairsOfUserAndRating).size();

                    List<Tuple2<String, Tuple3<String, Integer, Integer>>> tuples = new ArrayList<>();
                    pairsOfUserAndRating.forEach(item -> {
                        String user = item._1;
                        Integer rating = item._2;
                        tuples.add(new Tuple2<>(user, new Tuple3<>(movie, rating, numberOfRaters)));
                    });
                    return tuples.iterator();
                });
    }

    private static Tuple3<Double, Double, Double> calculateCorrelations(
            Iterable<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> records) {

        // how many point we have
        int groupSize = 0;
        int dotProduct = 0; // sum of ratingProduct: sum(x*y)
        int rating1Sum = 0; // sum of rating1 : sum(x)
        int rating2Sum = 0; // sum of rating2 : sum(y)
        int rating1NormSq = 0; // sum of rating1Squared :sum(x^2)
        int rating2NormSq = 0; // sum of rating2Squared :sum(y^2)
        int maxNumOfRaterS1 = 0; // max of numOfRaterS1 : max(numOfRater)
        int maxNumOfRaterS2 = 0; // max of numOfRaterS2 : max(numOfRater)

        for (Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> record : records) {
            groupSize++;
            dotProduct += record._5();
            rating1Sum += record._1();
            rating2Sum += record._3();
            rating1NormSq += record._6();
            rating2NormSq += record._7();

            int numOfRaterS1 = record._2();
            if (numOfRaterS1 > maxNumOfRaterS1) {
                maxNumOfRaterS1 = numOfRaterS1;
            }
            int numOfRaterS2 = record._4();
            if (numOfRaterS2 > maxNumOfRaterS2) {
                maxNumOfRaterS2 = numOfRaterS2;
            }
        }

        double pearson = calculatePearsonCorrelation(groupSize, dotProduct, rating1Sum, rating2Sum,
                rating1NormSq, rating2NormSq);

        double cosine = calculateCosineCorrelation(dotProduct,
                Math.sqrt(rating1NormSq), Math.sqrt(rating2NormSq));

        double jaccard = calculateJaccardCorrelation(groupSize, maxNumOfRaterS1, maxNumOfRaterS2);

        return new Tuple3<>(pearson, cosine, jaccard);
    }

    // how many pecentage of people rating for both movie
    private static double calculateJaccardCorrelation(int groupSize, int maxNumOfRaterS1, int maxNumOfRaterS2) {
        double union = maxNumOfRaterS1 + maxNumOfRaterS2 - groupSize;
        return groupSize / union;
    }

    private static double calculateCosineCorrelation(int dotProduct, double rating1Norm, double rating2Norm) {
        return dotProduct / (rating1Norm * rating2Norm);
    }

    private static double calculatePearsonCorrelation(int groupSize, int dotProduct,
                                                      int rating1Sum, int rating2Sum,
                                                      int rating1NormSq, int rating2NormSq) {

        double numerator = groupSize * dotProduct - rating1Sum * rating2Sum;
        double denominator = Math.sqrt(groupSize * rating1NormSq - rating1Sum * rating1Sum) *
                Math.sqrt(groupSize * rating2NormSq - rating2Sum * rating2Sum);
        return numerator / denominator;
    }


}
