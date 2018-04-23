package com.common;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class DateUtilTest {

    @Test
    void shouldReturnTimeStamp() {

        String dateStr = "2018-10-19";
        long timestamp = DateUtil.getTimestamp(dateStr);
        System.out.println(timestamp);
        //2004-11-04
        String dateAsString = DateUtil.getDateAsString(1073146260000L);
        assertThat(dateStr, is(dateAsString));

    }
}