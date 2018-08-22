package com.realtime.kafka;

import com.realtime.avro.Customer;
import com.realtime.common.Constants;
import com.realtime.common.UserGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

import static com.realtime.common.KafkaConfigUtil.createAvroProducerConfig;

public class AvroProducer2 {

    public static void main(String... args) {
        Properties props = createAvroProducerConfig();
        KafkaProducer<String, Customer> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String customerId = scanner.nextLine();
            Customer customer = UserGenerator.getCustomer(customerId);
            ProducerRecord<String, Customer> record = new ProducerRecord<>(Constants.KAFKA_TOPICS,
                    customer.getCustomerName().toString(), customer);
            producer.send(record, ((metadata, exception) -> {
                System.out.println(String.format("Save customer into %s", metadata.partition()));
            }));
        }

    }
}
