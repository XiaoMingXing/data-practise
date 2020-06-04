package com.realtime.common;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.realtime.avro.Order;
import org.bouncycastle.util.Arrays;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;

public class FileReader {

    private final static String fileName = "sample_sparkData.csv";
    private String filePath;


    public List<String> readData(int number) throws IOException {

        ArrayList<String> list = new ArrayList<>();

        String filePath = getFilePath();
        if (filePath == null) {
            return list;
        }
        File file = new File(filePath);

        int totalLineNumber = getTotalLineNumber();
        int[] lineNumbers = generateLineNumbers(number, totalLineNumber);
        int count = 0;

        try (Scanner scanner = new Scanner(file)) {
            while (scanner.hasNext() && list.size() < number) {
                String line = scanner.nextLine();
                count++;
                if (Arrays.contains(lineNumbers, count)) {
                    list.add(line);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return list;
    }


    public int[] generateLineNumbers(int count, int max) {
        Random random = new Random();
        IntStream sorted = random.ints(count, 1, max);
        return sorted.toArray();
    }

    private String getFilePath() {
        if (this.filePath == null) {
            URL resource = getClass().getClassLoader().getResource(fileName);
            if (resource == null) {
                return null;
            }
            this.filePath = resource.getFile();
        }
        return this.filePath;
    }

    int getTotalLineNumber() throws IOException {
        String filePath = this.getFilePath();
        if (filePath == null) {
            return 0;
        }
        InputStream is = new BufferedInputStream(new FileInputStream(filePath));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }


    public List<Order> getOrders() throws IOException {
        ArrayList<Order> orders = new ArrayList<>();

        Reader reader = Files.newBufferedReader(Paths.get(Objects.requireNonNull(this.getFilePath())));
        CsvToBean<CsvOrder> csvToBean = new CsvToBeanBuilder(reader)
                .withType(CsvOrder.class)
                .withIgnoreLeadingWhiteSpace(true)
                .build();


        Iterator<CsvOrder> csvOrders = csvToBean.iterator();
        while (csvOrders.hasNext()) {
            CsvOrder csvOrder = csvOrders.next();

            Order order = Order.newBuilder()
                    .setCustomerId(csvOrder.getCustomer_id())
                    .setBookingDestinationLatitude(csvOrder.getBooking_destination_latitude())
                    .setBookingDestinationLongitude(csvOrder.getBooking_destination_longitude())
                    .setBookingDestinationS2id(csvOrder.getBooking_destination_s2id())
                    .setBookingPickupToDestinationDistanceKm(csvOrder.getBooking_pickup_to_destination_distance_km())
                    .setBookingTime(csvOrder.getBooking_time())
                    .setCbv(csvOrder.getCbv())
                    .setCollection(csvOrder.getCollection())
                    .setDynamicSurgeFactor(csvOrder.getDynamic_surge_factor())
                    .setItemTotalCnt(csvOrder.getItem_total_cnt())
                    .setItemUniqueCnt(csvOrder.getItem_unique_cnt())
                    .setVoucherId(csvOrder.getVoucher_id())
                    .setMerchantCategory(csvOrder.getMerchant_category())
                    .setMerchantId(csvOrder.getMerchant_id())
                    .setMerchantLatitude(csvOrder.getMerchant_latitude())
                    .setMerchantLongitude(csvOrder.getMerchant_longitude())
                    .setMerchantSubCategory(csvOrder.getMerchant_sub_category())
                    .setNormalizedGmv(csvOrder.getNormalized_gmv())
                    .setPaymentMethodName(csvOrder.getPayment_method_name())
                    .setServiceAreaName(csvOrder.getService_area_name())
                    .setStatusName(csvOrder.getStatus_name())
                    .build();

            orders.add(order);
        }
        return orders;
    }
}
