package com.spark.recommendation;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.ClassTag;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class MovieRecommendationWithJoinTest extends SharedJavaSparkContext implements
        Serializable {

    private static final long serialVersionUID = -5681683598336701496L;


    public void shouldContainNumberOfRaters() {
        // Create and run the test
//        List<String> input = Arrays.asList("1\tHeart", "2\tDiamonds");
//        JavaRDD<String> inputRDD = jsc().parallelize(input);
//        JavaPairRDD<String, Tuple3<String, Integer, Integer>> result =
//                MovieRecommendationWithJoin.calculateNumberOfRaters(inputRDD);
//
//        // Create the expected output
//        List<Tuple2<String, Integer>> expectedInput = Arrays.asList(
//                new Tuple2<>("Heart", 1),
//                new Tuple2<>("Diamonds", 2));
//        JavaPairRDD<String, Integer> expectedRDD = jsc()
//                .parallelizePairs(expectedInput);
//
//        ClassTag<Tuple2<String, Integer>> tag =
//                scala.reflect.ClassTag$.MODULE$
//                        .apply(Tuple2.class);
//
//        // Run the assertions on the result and expected
//        JavaRDDComparisons.assertRDDEquals(
//                JavaRDD.fromRDD(JavaPairRDD.toRDD(result), tag),
//                JavaRDD.fromRDD(JavaPairRDD.toRDD(expectedRDD), tag));
    }
}