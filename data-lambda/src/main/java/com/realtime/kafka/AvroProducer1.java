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

import static com.realtime.common.KafkaConfigUtil.createAvroProducerConfig;

public class AvroProducer1 {

    public static void main(String... args) {
        Properties props = createAvroProducerConfig();
        KafkaProducer<String, User> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String userId = scanner.nextLine();
            User user = UserGenerator.getUser(userId);
            ProducerRecord<String, User> record = new ProducerRecord<>(Constants.KAFKA_TOPICS,
                    user.getUsername().toString(), user);
            producer.send(record, ((metadata, exception) -> {
                System.out.println(String.format("Save user into %s", metadata.partition()));
            }));
        }

    }
}
