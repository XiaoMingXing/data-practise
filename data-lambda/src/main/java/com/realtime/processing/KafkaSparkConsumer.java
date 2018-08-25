package com.realtime.processing;

import com.realtime.avro.User;
import com.realtime.common.Constants;
import com.realtime.common.KafkaConfigUtil;
import org.apache.avro.*;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLOutput;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

public class KafkaSparkConsumer {


    private void start() throws InterruptedException, IOException {

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("WorkCountStreaming");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));


        JavaInputDStream<ConsumerRecord<String, GenericData.Record>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(
                                Collections.singletonList(Constants.KAFKA_TOPICS),
                                KafkaConfigUtil.createSparkAvroConsumer("spark_streaming_consumer_group")));


        stream.map(ConsumerRecord::value)
                .foreachRDD(userJavaRDD -> {
                    userJavaRDD.foreachPartition((VoidFunction<Iterator<GenericData.Record>>) records -> {
                        SchemaValidator schemaValidator = new SchemaValidatorBuilder()
                                .canBeReadStrategy().validateLatest();
                        File file = FileUtils.getFile("data-lambda/src/main/avro/user.avsc");
                        Schema schema = new Schema.Parser().parse(file.getAbsoluteFile());
                        Set<Schema> schemas = Collections.singleton(schema);

                        while (records.hasNext()) {
                            GenericData.Record next = records.next();
                            try {
                                schemaValidator.validate(next.getSchema(), schemas);
                                System.out.println(next);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    });
                });
        jssc.start();              // Start the computation
        jssc.awaitTermination();
    }

    public static void main(String... args) throws InterruptedException, IOException {
        new KafkaSparkConsumer().start();
    }
}
