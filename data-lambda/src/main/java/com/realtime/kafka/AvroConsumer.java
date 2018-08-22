package com.realtime.kafka;

import com.google.common.collect.Lists;
import com.realtime.avro.User;
import com.realtime.common.Constants;
import com.realtime.common.KafkaConfigUtil;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;
import static com.realtime.common.KafkaConfigUtil.createAvroConsumerConfig;

public class AvroConsumer {

    public static void main(String... args) throws Exception {

        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(
                createAvroConsumerConfig("avroConsumerGroup"));
        consumer.subscribe(newArrayList(Constants.KAFKA_TOPICS));
        while (true) {
            ConsumerRecords<String, User> records = consumer.poll(600);
            records.forEach((record) -> {
                System.out.println("Offset:" + record.offset());
                System.out.println("Partition:" + record.partition());
                System.out.println("Key:" + record.key());
                System.out.println("Value:" + record.value());
            });
            consumer.commitSync();
        }
    }
}
