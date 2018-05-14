package com.spark.recommendation;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.ClassTag;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class MovieRecommendationWithJoinTest extends SharedJavaSparkContext implements
        Serializable {

    private static final long serialVersionUID = -5681683598336701496L;


    @Test
    public void shouldContainNumberOfRaters() {

        List<String> input = Arrays.asList("User1 Movie1 3", "User1 Movie2 4");
        JavaRDD<String> inputRDD = jsc().parallelize(input);
        JavaPairRDD<String, Tuple3<String, Integer, Integer>> result =
                MovieRecommendationWithJoin.calculateNumberOfRaters(inputRDD);

        // Create the expected output
        List<Tuple2<String, Tuple3<String, Integer, Integer>>> expectedInput = Arrays.asList(
                new Tuple2<>("User1", new Tuple3<>("Movie1", 3, 1)),
                new Tuple2<>("User1", new Tuple3<>("Movie2", 4, 1)));
        JavaPairRDD<String, Tuple3<String, Integer, Integer>> expectedRDD = jsc()
                .parallelizePairs(expectedInput);

        ClassTag<Tuple2<String, Tuple3<String, Integer, Integer>>> tag =
                scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class);

        result.collect().forEach(System.out::println);

        // Run the assertions on the result and expected
        JavaRDDComparisons.assertRDDEquals(
                JavaRDD.fromRDD(JavaPairRDD.toRDD(result), tag),
                JavaRDD.fromRDD(JavaPairRDD.toRDD(expectedRDD), tag));
    }
}