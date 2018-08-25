package com.realtime.kafka;

import com.realtime.avro.Order;
import com.realtime.avro.User;
import com.realtime.common.Constants;
import com.realtime.common.OrderGenerator;
import com.realtime.common.UserGenerator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import static com.realtime.common.KafkaConfigUtil.createAvroProducerConfig;

public class OrderProducer {

    public static void main(String... args) {
        Properties props = createAvroProducerConfig();
        final KafkaProducer<String, Order> producer = new KafkaProducer<>(props);

        Scanner scanner = new Scanner(System.in);
        while (true) {
            int count = scanner.nextInt();

            List<Order> orders = OrderGenerator.generateOrders(count);
            System.out.println(String.format("Pick up %s orders...", orders.size()));

            if (!CollectionUtils.isEmpty(orders)) {
                for (Order order : orders) {
                    ProducerRecord<String, Order> record = new ProducerRecord<>(Constants.KAFKA_TOPICS,
                            order.getCustomerId().toString(), order);
                    producer.send(record, ((metadata, exception) -> {
                        System.out.println(String.format("Save order %s into %s", order.getCustomerId(),
                                metadata.partition()));
                    }));
                }
            }
        }
    }
}
