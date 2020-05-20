package com.realtime.generator;

import com.realtime.common.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class OrderGenerator {

    private List<String[]> orders;
    private Random rand = new Random();

    public List<SalesOrder> generateOrdersFromFile(int numOfRecords) {
        if (orders == null) {
            orders = Utils.readCsv("sample_data/sales_order.csv");
        }

        List<SalesOrder> result = new ArrayList<>();
        for (int i = 0; i < numOfRecords; i++) {
            int randomIndex = rand.nextInt(orders.size());
            String[] randomElement = orders.get(randomIndex);
            result.add(SalesOrder.parse(randomElement));

        }
        return result;
    }


    public static void main(String[] args) {
        OrderGenerator orderGenerator = new OrderGenerator();
        List<SalesOrder> salesOrders = orderGenerator.generateOrdersFromFile(10);

        System.out.println(salesOrders.size());
    }
}
