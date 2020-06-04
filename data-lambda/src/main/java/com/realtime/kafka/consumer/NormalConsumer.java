package com.realtime.kafka.consumer;

import com.realtime.common.Constants;
import com.realtime.common.KafkaConfigUtil;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class NormalConsumer {

    public static void main(String... args) throws Exception {

        Properties props = KafkaConfigUtil
                .createConsumerConfig("normalConsumerGroup");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(Constants.KAFKA_TOPICS));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record -> {
                System.out.println("=======" + record.key());
                System.out.println("=======" + record.value());
            });
            consumer.commitAsync();
        }

    }
}
