package com.kafka;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class SchemaMainTest {

    private OkHttpClient client;

    private final static String SCHEMA_SERVER = "54.169.78.185";

    @Before
    public void setUp() throws Exception {
        client = new OkHttpClient();
    }

    @Test
    public void should_list_the_latest() throws IOException {
        //SHOW THE LATEST VERSION OF EMPLOYEE 2
        Request request = new Request.Builder()
                .url(String.format("http://%s:8081/subjects/Employee/versions/latest", SCHEMA_SERVER))
                .build();
        String output = client.newCall(request).execute().body().string();
        System.out.println(output);
    }
}