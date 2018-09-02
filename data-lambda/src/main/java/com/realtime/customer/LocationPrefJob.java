package com.realtime.customer;

import com.realtime.common.Constants;
import com.realtime.common.KafkaConfigUtil;
import com.realtime.processing.SchemaBroadcast;
import org.apache.avro.Schema;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

public class LocationPrefJob {


    private void start() throws InterruptedException, IOException {

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Location Preference Job");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));


        JavaInputDStream<ConsumerRecord<String, GenericData.Record>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(
                                Collections.singletonList(Constants.KAFKA_TOPICS),
                                KafkaConfigUtil.createSparkAvroConsumer("location_pref_group")));


        stream.map(ConsumerRecord::value)
                .foreachRDD(userJavaRDD -> {
                    userJavaRDD.foreachPartition((VoidFunction<Iterator<GenericData.Record>>) records -> {

                        // get HBase connection

                        // normalize Records

                        // update HBase connection

                        while (records.hasNext()) {
                            GenericData.Record next = records.next();
                        }
                    });
                });
        jssc.start();
        jssc.awaitTermination();
    }

    public static void main(String... args) throws InterruptedException, IOException {
        new LocationPrefJob().start();
    }

}


