package com.realtime.common;

import com.realtime.avro.Order;
import org.bouncycastle.util.Arrays;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OrderGenerator {

    public static List<Order> generateOrders(int number) {
        try {
            FileReader fileReader = new FileReader();
            List<Order> orders = fileReader.getOrders();

            List<Order> list = new ArrayList<>();
            int count = orders.size();
            int[] lineNumbers = fileReader.generateLineNumbers(number, count);
            for (int index = 0; list.size() < number && index < count; index++) {
                if (Arrays.contains(lineNumbers, index)) {
                    list.add(orders.get(index));
                }
            }
            return list;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

}
