package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkContextBuilder {

    public static JavaSparkContext createJavaSparkContext() throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("market basket analysis")
                .setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryoserializer.buffer.mb", "32");
        return new JavaSparkContext(conf);


    }
}
