package com.realtime.processing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;


import java.util.*;

public class KafkaSparkStreaming {

    private Map<String, Object> kafkaParams;

    public KafkaSparkStreaming() {
        this.kafkaParams = new HashMap<>();

        this.kafkaParams.put("bootstrap.servers", "18.136.30.81:9092,52.221.68.56:9092");
        this.kafkaParams.put("key.deserializer", StringDeserializer.class);
        this.kafkaParams.put("value.deserializer", StringDeserializer.class);
        this.kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");

        this.kafkaParams.put("auto.offset.reset", "earliest");
        this.kafkaParams.put("enable.auto.commit", false);
    }

    private void start() throws InterruptedException {
        List<String> topics = Arrays.asList("input-topic", "click-stream");

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("WorkCountStreaming");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, this.kafkaParams));


        JavaDStream<String> words = stream
                .map(ConsumerRecord::value)
                .flatMap(record -> Arrays.asList(record.split(" ")).iterator());

        JavaPairDStream<String, Integer> wordCounts =
                words.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((a, b) -> a + b);

        wordCounts.foreachRDD(rdd -> {
            rdd.foreachPartition((VoidFunction<Iterator<Tuple2<String, Integer>>>) tuple2 -> {
                while (tuple2.hasNext()) {
                    System.out.println(tuple2.next());
                }
            });
        });

        jssc.start();              // Start the computation
        jssc.awaitTermination();
    }

    public static void main(String... args) throws InterruptedException {
        new KafkaSparkStreaming().start();
    }
}
