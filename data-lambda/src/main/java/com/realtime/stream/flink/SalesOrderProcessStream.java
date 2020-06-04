package com.realtime.stream.flink;

import com.realtime.common.Constants;
import com.realtime.common.KafkaConfigUtil;
import com.realtime.generator.SalesOrder;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import scala.Tuple2;

import java.util.Objects;
import java.util.Properties;

public class SalesOrderProcessStream {

    public static void main(String[] args) throws Exception {

        Properties properties = KafkaConfigUtil.getFlinkConsumerConfig();

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer<String> kafkaConsumer =
                new FlinkKafkaConsumer<>(Constants.KAFKA_TOPICS,
                new SimpleStringSchema(), properties);

        kafkaConsumer.setStartFromEarliest();

        DataStream<String> transactions = env
                .addSource(kafkaConsumer)
                .name("sales transactions");

        SingleOutputStreamOperator<Tuple2<String, String>> alerts = transactions
                .map((MapFunction<String, SalesOrder>) SalesOrder::parseForRealtime)
                .filter((FilterFunction<SalesOrder>) Objects::nonNull)
                .keyBy(SalesOrder::getSellerId)
                .process(new FraudDetector())
                .name("fraud-detector");


        alerts.print();

//        alerts
//                .addSink(FlinkStreamSinkFactory.getDefaultFileSink())
//                .setParallelism(1)
//                .name("send alerts");

        env.execute("Fraud detection");
    }
}
