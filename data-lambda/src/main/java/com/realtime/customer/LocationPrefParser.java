package com.realtime.customer;

import org.apache.avro.generic.GenericData;

import java.util.Iterator;

class LocationPrefParser {

    void normalizeRecords(Iterator<GenericData.Record> records) {
        records.forEachRemaining(record -> normalizeRecord(record));
    }

    private void normalizeRecord(GenericData.Record record) {
        System.out.println(record.toString());
    }
}
