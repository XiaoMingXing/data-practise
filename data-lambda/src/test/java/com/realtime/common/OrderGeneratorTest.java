package com.realtime.common;

import com.realtime.avro.Order;
import com.realtime.generator.OrderGenerator;
import org.apache.avro.generic.GenericData;
import org.junit.Test;

import java.util.List;

public class OrderGeneratorTest {

    @Test
    public void testGenerateOrders() {

        List<Order> orders = OrderGenerator.generateOrders(20);
        System.out.println("Order:" + orders.size());

    }

    @Test
    public void testGenerateGenericRecords() {

        List<GenericData.Record> records = OrderGenerator.generateRecords(10);

        records.forEach(record -> {
            System.out.println(record.toString());
        });

    }
}