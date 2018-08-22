package com.realtime.common;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.directory.api.util.Strings;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaConfigUtil {


    public static Properties createAvroConsumerConfig(String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.KAFKA_BROKERS);
        props.put("key.deserializer", KafkaAvroDeserializer.class);
        props.put("value.deserializer", KafkaAvroDeserializer.class);

        if (Strings.isEmpty(groupId)) {
            groupId = "defaultConsumerGroup";
        }
        props.put("group.id", groupId);
        props.put("schema.registry.url", Constants.SCHEMA_REGISTRY_URL);
        return props;
    }


    public static Properties createAvroProducerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.KAFKA_BROKERS);
        props.put("key.serializer", KafkaAvroSerializer.class);
        props.put("value.serializer", KafkaAvroSerializer.class);
        props.put("schema.registry.url", Constants.SCHEMA_REGISTRY_URL);
        return props;
    }

    public static Properties createConsumerConfig(String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.KAFKA_BROKERS);
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);
        return props;
    }
}
