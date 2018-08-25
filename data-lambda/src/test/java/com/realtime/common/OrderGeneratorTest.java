package com.realtime.common;

import com.realtime.avro.Order;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class OrderGeneratorTest {

    @Test
    public void testGenerateOrders() {

        List<Order> orders = OrderGenerator.generateOrders(20);
        System.out.println("Order:" + orders.size());

    }
}