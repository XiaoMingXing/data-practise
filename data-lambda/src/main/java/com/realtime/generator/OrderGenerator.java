package com.realtime.generator;

import com.realtime.avro.Order;
import com.realtime.common.Constants;
import com.realtime.common.FileReader;
import com.realtime.common.Utils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bouncycastle.util.Arrays;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class OrderGenerator {

    private List<String[]> orders;
    private Random rand = new Random();

    private static Log logger = LogFactory.getLog(OrderGenerator.class);

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

    public static List<GenericData.Record> generateRecords(int number) {
        List<Order> orders = generateOrders(number);

        return orders.stream()
                .map(OrderGenerator::mapToGenericRecord)
                .collect(Collectors.toList());
    }

    private static GenericData.Record mapToGenericRecord(Order order) {
        try {
            Schema schema = getOrderSchema();
            GenericData.Record record = new GenericData.Record(schema);
            record.put("customer_id", order.getCustomerId());
            record.put("service_area_name", order.getServiceAreaName());
            record.put("booking_time", order.getBookingTime());
            record.put("merchant_category", order.getMerchantCategory());
            record.put("merchant_sub_category", order.getMerchantSubCategory());
            record.put("collection", order.getCollection());
            record.put("merchant_id", order.getMerchantId());
            record.put("status_name", order.getStatusName());
            record.put("voucher_id", order.getVoucherId());
            record.put("merchant_latitude", order.getMerchantLatitude());
            record.put("merchant_longitude", order.getMerchantLongitude());
            record.put("booking_destination_latitude", order.getBookingDestinationLatitude());
            record.put("booking_destination_longitude", order.getBookingDestinationLongitude());
            record.put("booking_destination_s2id", order.getBookingDestinationS2id());
            record.put("booking_pickup_to_destination_distance_km", order.getBookingPickupToDestinationDistanceKm());
            record.put("normalized_gmv", order.getNormalizedGmv());
            record.put("cbv", order.getCbv());
            record.put("item_total_cnt", order.getItemTotalCnt());
            record.put("item_unique_cnt", order.getItemUniqueCnt());
            record.put("dynamic_surge_factor", order.getDynamicSurgeFactor());
            record.put("payment_method_name", order.getPaymentMethodName());
            return record;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

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


    private static Schema getOrderSchema() throws IOException {
        InputStream avroFile = getAvroFile(Constants.SCHEMA_NAME);
        return new Schema.Parser().parse(avroFile);
    }

    public static InputStream getAvroFile(String name) {
        return OrderGenerator.class.getClassLoader().getResourceAsStream(name);
    }

    public static void main(String[] args) {
        OrderGenerator orderGenerator = new OrderGenerator();
        List<SalesOrder> salesOrders = orderGenerator.generateOrdersFromFile(10);

        System.out.println(salesOrders.size());
    }

}
