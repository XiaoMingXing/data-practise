package com.realtime.processing;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.File;
import java.io.IOException;

public class SchemaBroadcast {

    private static volatile Broadcast<String> instance = null;

    static Broadcast<String> getInstance(JavaSparkContext jsc) {
        if (instance == null) {
            synchronized (SchemaBroadCast.class) {
                if (instance == null) {
                    File file = FileUtils.getFile("data-lambda/src/main/avro/user.avsc");
                    try {
                        Schema schema = new Schema.Parser().parse(file.getAbsoluteFile());
                        instance = jsc.broadcast(schema.toString());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            }
        }
        return instance;
    }
}
