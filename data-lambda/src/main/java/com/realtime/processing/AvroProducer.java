package com.realtime.processing;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class AvroProducer {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "18.136.30.81:9092,52.221.68.56:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(SCHEMAS.USER_SCHEMA1);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 5; i++) {
            GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("aaa", "aaa-" + i);
            avroRecord.put("bbb", "bbb-" + i);
            avroRecord.put("address", "add-" + i);

            byte[] bytes = recordInjection.apply(avroRecord);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>("click-stream", bytes);
            producer.send(record);

            Thread.sleep(250);

        }

        producer.close();
    }
}