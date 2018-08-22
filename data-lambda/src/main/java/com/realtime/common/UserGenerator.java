package com.realtime.common;

import com.realtime.avro.Customer;
import com.realtime.avro.User;

import java.util.Random;

public class UserGenerator {

    private static Random random = new Random();

    public static User getUser(String userId) {
        return User.newBuilder()
                .setUserId(userId)
                .setUsername("xiao" + random.nextInt())
                .setEmail("920@qq.com")
                .setAddress("mockAddress")
                .build();
    }

    public static Customer getCustomer(String customerId) {
        return Customer.newBuilder()
                .setCustomerId(customerId)
                .setUserId(customerId)
                .setCustomerName("CustomerName")
                .setEmail("xiao@gmail.com")
                .setAddress("newAddress")
                .build();
    }
}
