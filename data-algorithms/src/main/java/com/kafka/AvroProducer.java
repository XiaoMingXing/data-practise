package com.kafka;


import com.avro.clickstreams.Employee;
import com.avro.clickstreams.PhoneNumber;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.hadoop.io.AvroSerializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.IntStream;

public class AvroProducer {

    private static Producer<Long, Employee> createProducer() {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "52.221.241.212:9092,52.221.241.212:9093");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://54.169.78.185:8081");
        return new KafkaProducer<>(props);
    }

    private final static String TOPIC = "input-topic";

    public static void main(String... args) throws IOException {
        Producer<Long, Employee> producer = createProducer();

        Employee bob = Employee.newBuilder().setAge(35)
                .setFirstName("Bob")
                .setLastName("Jones")
                .setPhoneNumber(
                        PhoneNumber.newBuilder()
                                .setAreaCode("301")
                                .setCountryCode("1")
                                .setPrefix("555")
                                .setNumber("1234")
                                .build())
                .build();
        IntStream.range(1, 100).forEach(index -> {
            producer.send(new ProducerRecord<>(TOPIC, 1L * index, bob));
        });
        producer.flush();
        producer.close();

    }

}
