package com.kafka;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

import java.io.IOException;

public class SchemaMain {


    private final static MediaType SCHEMA_CONTENT =
            MediaType.parse("application/vnd.schemaregistry.v1+json");

    private final static String SCHEMA_SERVER = "54.169.78.185";

    private final static String EMPLOYEE_SCHEMA = "{\n" +
            "  \"schema\": \"" +
            "  {" +
            "    \\\"namespace\\\": \\\"com.cloudurable.phonebook\\\"," +
            "    \\\"type\\\": \\\"record\\\"," +
            "    \\\"name\\\": \\\"Employee\\\"," +
            "    \\\"fields\\\": [" +
            "        {\\\"name\\\": \\\"fName\\\", \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"lName\\\", \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"age\\\",  \\\"type\\\": \\\"int\\\"}," +
            "        {\\\"name\\\": \\\"phoneNumber\\\",  \\\"type\\\": \\\"string\\\"}" +
            "    ]" +
            "  }\"" +
            "}";

    public static void main(String... args) throws IOException {


        final OkHttpClient client = new OkHttpClient();

        //POST A NEW SCHEMA
        String requestURL = String.format("http://%s:8081/subjects/Employee/versions", SCHEMA_SERVER);
        Request request = new Request.Builder()
                .post(RequestBody.create(SCHEMA_CONTENT, EMPLOYEE_SCHEMA))
                .url(requestURL)
                .build();

        String output = client.newCall(request).execute().body().string();
        System.out.println(output);

        //SHOW VERSION 2 OF EMPLOYEE
        request = new Request.Builder()
                .url(String.format("http://%s:8081/subjects/Employee/versions/2", SCHEMA_SERVER))
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);
    }

}
