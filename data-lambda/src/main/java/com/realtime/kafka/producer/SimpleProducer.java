package com.realtime.kafka.producer;

import com.realtime.common.Constants;
import com.realtime.generator.LogGenerator;
import com.realtime.generator.OrderGenerator;
import com.realtime.generator.SalesOrder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import static com.realtime.common.KafkaConfigUtil.createSimpleProducerConfig;

public class SimpleProducer {

    public static void main(String... args) {


        Scanner scanner = new Scanner(System.in);
        while (true) {
            int count = scanner.nextInt();
//            List records = getMockOrders(count);
            List records = getMockUserBehaviour(count);
            sendToTopic(records);
        }
    }

    private static List getMockUserBehaviour(int count) {
        return LogGenerator.readRandomLines(count);
    }

    private static List getMockOrders(int count) {
        OrderGenerator orderGenerator = new OrderGenerator();
        return orderGenerator.generateOrdersFromFile(count);
    }

    private static void sendToTopic(List records) {

        Properties props = createSimpleProducerConfig();
        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        System.out.println(String.format("Pick up %s orders...", records.size()));

        if (!CollectionUtils.isEmpty(records)) {
            for (Object data : records) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(Constants.KAFKA_TOPICS_LOG, data.toString());
                producer.send(record, ((metadata, exception) -> {
                    System.out.println(String.format("Save one record into %s",
                            metadata.partition()));
                }));
            }
        }
    }
}
