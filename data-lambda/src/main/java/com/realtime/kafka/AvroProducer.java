package com.realtime.kafka;

import com.realtime.avro.User;
import com.realtime.common.Constants;
import com.realtime.common.UserGenerator;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Console;
import java.util.Properties;
import java.util.Scanner;

public class AvroProducer {

    public static void main(String... args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.KAFKA_BROKERS);
        props.put("key.serializer", KafkaAvroSerializer.class);
        props.put("value.serializer", KafkaAvroSerializer.class);
        props.put("schema.registry.url", Constants.SCHEMA_REGISTRY_URL);

        KafkaProducer<String, User> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String userId = scanner.nextLine();
            User user = UserGenerator.getUser(userId);
            ProducerRecord<String, User> record = new ProducerRecord<>(Constants.KAFKA_TOPICS,
                    user.getUsername().toString(), user);
            producer.send(record, ((metadata, exception) -> {
                System.out.println(String.format("Save into %s", metadata.partition()));
            }));
        }

    }
}