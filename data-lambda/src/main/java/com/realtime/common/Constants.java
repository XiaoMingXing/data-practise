package com.realtime.common;

import java.util.Collection;

public class Constants {

    public static final String KAFKA_BROKERS = "localhost:9092";
    public static final String ZK_SERVERS = "localhost:2181";
//    public static final String KAFKA_BROKERS = "18.136.30.81:9092,52.221.68.56:9092";
    public static final String KAFKA_TOPICS = "click-com.realtime.stream";
    public static final String KAFKA_TOPIC_REGEX = "\\s+(-?com.realtime.stream)";
    public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
//    public static final String SCHEMA_REGISTRY_URL = "http://13.229.105.134:8081";
    public static final String SCHEMA_NAME = "avro/order.avsc";
}
