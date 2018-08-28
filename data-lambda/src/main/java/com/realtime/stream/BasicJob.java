package com.realtime.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class BasicJob {

    public static void main(String... args) throws Exception {

        SparkConf config = new SparkConf()
                .setAppName("BasicStreamingJob")
                .setMaster("local[*]");

        JavaStreamingContext streamingContext =
                new JavaStreamingContext(config, Durations.seconds(2));

        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("localhost",
                9087, StorageLevel.MEMORY_AND_DISK());


        JavaPairDStream<String, Integer> wordcount = lines
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((count1, count2) -> count1 + count2);
        wordcount.foreachRDD(rdd -> rdd.foreach(record -> System.out.println(record.toString())));

        streamingContext.start();
        streamingContext.awaitTermination();
    }

}
