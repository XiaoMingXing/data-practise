package com.realtime.stream.flink;

import com.realtime.common.Constants;
import com.realtime.common.KafkaConfigUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;

public class SalesOrderProcessTable {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useAnyPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        tableEnv
                .connect(new Kafka()
                        .version("universal")
                        .topic(Constants.KAFKA_TOPICS)
                        .properties(KafkaConfigUtil.getFlinkConsumerConfig())
                        .startFromEarliest()
                )
                .withFormat(new Json())
                .withSchema(SchemaUtil.getSalesOrderSchema())
                .registerTableSource("salesOrderVIew");


        tableEnv
                .connect(new Kafka()
                        .version("universal")
                        .topic("result_table")
                        .properties(KafkaConfigUtil.getFlinkConsumerConfig())
                ).withFormat(new Json())
                .inUpsertMode()
                .withSchema(SchemaUtil.getSalesOrderResultSchema())
                .registerTableSink("sinkTopic");


        String sql =
                "INSERT INTO sinkTopic SELECT sellerId,SUM(actualGmv) as revenue FROM salesOrderVIew GROUP BY sellerId";

        tableEnv.sqlUpdate(sql);

//        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(table, Row.class);
//        table.printSchema();
//        dataStream.print();

        tableEnv.execute("My first table job");
    }
}
