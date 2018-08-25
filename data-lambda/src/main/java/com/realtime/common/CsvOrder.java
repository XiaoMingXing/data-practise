package com.realtime.common;

import com.opencsv.bean.CsvBindByName;
import org.apache.commons.lang.StringUtils;

public class CsvOrder {

    @CsvBindByName
    private String customer_id;
    @CsvBindByName
    private String service_area_name;
    @CsvBindByName
    private String booking_time;
    @CsvBindByName
    private String merchant_category;
    @CsvBindByName
    private String merchant_sub_category;
    @CsvBindByName
    private String collection;
    @CsvBindByName
    private String merchant_id;
    @CsvBindByName
    private String status_name;
    @CsvBindByName
    private String voucher_id;
    @CsvBindByName
    private String merchant_latitude;
    @CsvBindByName
    private String merchant_longitude;
    @CsvBindByName
    private String booking_destination_latitude;
    @CsvBindByName
    private String booking_destination_longitude;
    @CsvBindByName
    private String booking_destination_s2id;
    @CsvBindByName
    private double booking_pickup_to_destination_distance_km;
    @CsvBindByName
    private double normalized_gmv;
    @CsvBindByName
    private float cbv;
    @CsvBindByName
    private float item_total_cnt;
    @CsvBindByName
    private float item_unique_cnt;
    @CsvBindByName
    private float dynamic_surge_factor;
    @CsvBindByName
    private String payment_method_name;

    public String getCustomer_id() {
        return customer_id;
    }

    public void setCustomer_id(String customer_id) {
        this.customer_id = customer_id;
    }

    public String getService_area_name() {
        return service_area_name;
    }

    public void setService_area_name(String service_area_name) {
        this.service_area_name = service_area_name;
    }

    public String getBooking_time() {
        return booking_time;
    }

    public void setBooking_time(String booking_time) {
        this.booking_time = booking_time;
    }

    public String getMerchant_category() {
        return merchant_category;
    }

    public void setMerchant_category(String merchant_category) {
        this.merchant_category = merchant_category;
    }

    public String getMerchant_sub_category() {
        if (StringUtils.isEmpty(merchant_category)) {
            return "";
        }
        return merchant_sub_category;
    }

    public void setMerchant_sub_category(String merchant_sub_category) {
        this.merchant_sub_category = merchant_sub_category;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getMerchant_id() {
        return merchant_id;
    }

    public void setMerchant_id(String merchant_id) {
        this.merchant_id = merchant_id;
    }

    public String getStatus_name() {
        return status_name;
    }

    public void setStatus_name(String status_name) {
        this.status_name = status_name;
    }

    public String getVoucher_id() {
        return voucher_id;
    }

    public void setVoucher_id(String voucher_id) {
        this.voucher_id = voucher_id;
    }

    public String getMerchant_latitude() {
        return merchant_latitude;
    }

    public void setMerchant_latitude(String merchant_latitude) {
        this.merchant_latitude = merchant_latitude;
    }

    public String getMerchant_longitude() {
        return merchant_longitude;
    }

    public void setMerchant_longitude(String merchant_longitude) {
        this.merchant_longitude = merchant_longitude;
    }

    public String getBooking_destination_latitude() {
        return booking_destination_latitude;
    }

    public void setBooking_destination_latitude(String booking_destination_latitude) {
        this.booking_destination_latitude = booking_destination_latitude;
    }

    public String getBooking_destination_longitude() {
        return booking_destination_longitude;
    }

    public void setBooking_destination_longitude(String booking_destination_longitude) {
        this.booking_destination_longitude = booking_destination_longitude;
    }

    public String getBooking_destination_s2id() {
        return booking_destination_s2id;
    }

    public void setBooking_destination_s2id(String booking_destination_s2id) {
        this.booking_destination_s2id = booking_destination_s2id;
    }

    public double getBooking_pickup_to_destination_distance_km() {
        return booking_pickup_to_destination_distance_km;
    }

    public void setBooking_pickup_to_destination_distance_km(double booking_pickup_to_destination_distance_km) {
        this.booking_pickup_to_destination_distance_km = booking_pickup_to_destination_distance_km;
    }

    public double getNormalized_gmv() {
        return normalized_gmv;
    }

    public void setNormalized_gmv(double normalized_gmv) {
        this.normalized_gmv = normalized_gmv;
    }

    public float getCbv() {
        return cbv;
    }

    public void setCbv(float cbv) {
        this.cbv = cbv;
    }

    public float getItem_total_cnt() {
        return item_total_cnt;
    }

    public void setItem_total_cnt(float item_total_cnt) {
        this.item_total_cnt = item_total_cnt;
    }

    public float getItem_unique_cnt() {
        return item_unique_cnt;
    }

    public void setItem_unique_cnt(float item_unique_cnt) {
        this.item_unique_cnt = item_unique_cnt;
    }

    public float getDynamic_surge_factor() {
        return dynamic_surge_factor;
    }

    public void setDynamic_surge_factor(float dynamic_surge_factor) {
        this.dynamic_surge_factor = dynamic_surge_factor;
    }

    public String getPayment_method_name() {
        return payment_method_name;
    }

    public void setPayment_method_name(String payment_method_name) {
        this.payment_method_name = payment_method_name;
    }
}
