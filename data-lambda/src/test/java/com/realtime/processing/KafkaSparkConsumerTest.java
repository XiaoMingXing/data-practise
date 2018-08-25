package com.realtime.processing;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;

import static org.junit.Assert.*;

public class KafkaSparkConsumerTest {


    @Test
    public void testFileRead() {

        String fileName = "user.avsc";
        File file = FileUtils.getFile(fileName);
        System.out.println(file.getAbsoluteFile());


    }
}