package com.realtime.processing;

public class SCHEMAS {

    public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"com.person\","
            + "\"fields\":["
            + "  { \"name\":\"username\", \"type\":\"string\" },"
            + "  { \"name\":\"email\", \"type\":\"string\" },"
            + "  { \"name\":\"address\", \"type\":\"string\" }"
            + "]}";

    public static final String USER_SCHEMA1 = "{"
            + "\"type\":\"record\","
            + "\"name\":\"com.persona\","
            + "\"fields\":["
            + "  { \"name\":\"aaa\", \"type\":\"string\" },"
            + "  { \"name\":\"bbb\", \"type\":\"string\" },"
            + "  { \"name\":\"address\", \"type\":\"string\" }"
            + "]}";
}
