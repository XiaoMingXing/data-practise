package com.realtime.customer;

import com.realtime.generator.OrderGenerator;
import org.apache.avro.generic.GenericData;
import org.junit.Test;

import java.util.Iterator;

public class LocationPrefParserTest {


    @Test
    public void shouldParseRecords() {

        LocationPrefParser locationPrefParser = new LocationPrefParser();

        locationPrefParser.normalizeRecords(getMockRecords());
    }

    private Iterator<GenericData.Record> getMockRecords() {
        return OrderGenerator.generateRecords(10).iterator();
    }
}