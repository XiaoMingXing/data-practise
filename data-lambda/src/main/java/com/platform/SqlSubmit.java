package com.platform;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;


public class SqlSubmit {

    public static final String DELEMITER = ";";

    public static void submitJob(String jobFilePath) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        List<SqlCommandCall> calls =
                readSqlCommands(jobFilePath);

        for (SqlCommandCall call : calls) {
            switch (call.command) {
                case SET:
                    String key = call.operands[0];
                    String value = call.operands[1];
                    // 设置参数
                    tEnv.getConfig().getConfiguration().setString(key, value);
                    break;
                case CREATE_TABLE:
                case INSERT_INTO:
                    String ddl = call.operands[0];
                    tEnv.sqlUpdate(ddl);
                    break;
                default:
                    throw new RuntimeException("Unsupported command: " + call.command);
            }
        }
        tEnv.execute("SQL job");
    }


    public static List<SqlCommandCall> readSqlCommands(String path) {
        try {
            URL resource = SqlSubmit.class.getClassLoader()
                    .getResource(path);
            if (resource == null) {
                return null;
            }
            String sql = new String(Files.readAllBytes(Paths.get(resource.getPath())));

            String[] parts = sql.split(DELEMITER);
            return Arrays.stream(parts)
                    .map(s ->
                            SqlCommandParser.parse(s).orElse(null))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        String jobName = "jobs/ads_kafka_test";
        submitJob(jobName);
//        List<SqlCommandCall> calls =
//                readSqlCommands(jobName);
//
//        System.out.println(calls.size());
    }

}
