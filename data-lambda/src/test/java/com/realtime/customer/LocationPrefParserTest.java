package com.realtime.customer;

import com.realtime.common.OrderGenerator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.net.URL;
import java.util.Iterator;

import static org.junit.Assert.*;

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