package com.common;

import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

    private final static String DATE_FORMATE = "yyyy-mm-dd";
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat(DATE_FORMATE);

    public static long getTimestamp(String date) {
        if (StringUtils.isNotBlank(date)) {
            try {
                return SIMPLE_DATE_FORMAT.parse(date).getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return 0;
    }

    public static String getDateAsString(long timestamp) {
        return SIMPLE_DATE_FORMAT.format(new Date(timestamp));
    }
}
