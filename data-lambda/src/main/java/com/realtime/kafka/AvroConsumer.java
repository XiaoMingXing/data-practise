package com.realtime.kafka;

import com.google.common.collect.Lists;
import com.realtime.avro.User;
import com.realtime.common.Constants;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;

public class AvroConsumer {

    public static void main(String... args) throws Exception {

        Properties props = new Properties();

        props.put("bootstrap.servers", Constants.KAFKA_BROKERS);
        props.put("key.deserializer", KafkaAvroDeserializer.class);
        props.put("value.deserializer", KafkaAvroDeserializer.class);
        props.put("group.id", "avroConsumerGroup");

        props.put("schema.registry.url", Constants.SCHEMA_REGISTRY_URL);

        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(newArrayList(Constants.KAFKA_TOPICS));

        System.out.println("Reading from topic:" + Constants.KAFKA_TOPICS);

        while (true) {
            ConsumerRecords<String, User> records = consumer.poll(600);
            records.forEach((record) -> System.out.println(record.key() + ":" + record.value()));
            consumer.commitSync();
        }
    }
}
