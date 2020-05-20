package com.realtime.generator;

import java.util.Date;

public class SalesOrder {

    private String salesOrderId;
    private String salesOrderItemId;
    private String orderCreateDate;
    private String orderFulfillmentDate;
    private String skuId;
    private String sellerId;
    private String buyerId;
    private double actualGmv;
    private String venture;
    private double unitPrice;
    private double paidPrice;

    public static SalesOrder parse(String[] orderInfo) {
        SalesOrder salesOrder = new SalesOrder();
        salesOrder.setSalesOrderItemId(orderInfo[0]);
        salesOrder.setSalesOrderId(orderInfo[1]);
        salesOrder.setActualGmv("".equals(orderInfo[108]) ? 0 : Double.parseDouble(orderInfo[108]));
        salesOrder.setBuyerId(orderInfo[85]);
        salesOrder.setSellerId(orderInfo[79]);
        salesOrder.setOrderCreateDate(orderInfo[6]);
        salesOrder.setOrderFulfillmentDate(orderInfo[9]);
        salesOrder.setPaidPrice("".equals(orderInfo[14]) ? 0 : Double.parseDouble(orderInfo[14]));
        salesOrder.setVenture(orderInfo[134]);
        salesOrder.setSkuId(orderInfo[46]);
        salesOrder.setUnitPrice(orderInfo[13].equals("") ? 0 : Double.parseDouble(orderInfo[13]));
        return salesOrder;
    }


    public String getSalesOrderId() {
        return salesOrderId;
    }

    public void setSalesOrderId(String salesOrderId) {
        this.salesOrderId = salesOrderId;
    }

    public String getSalesOrderItemId() {
        return salesOrderItemId;
    }

    public void setSalesOrderItemId(String salesOrderItemId) {
        this.salesOrderItemId = salesOrderItemId;
    }

    public String getOrderCreateDate() {
        return orderCreateDate;
    }

    public void setOrderCreateDate(String orderCreateDate) {
        this.orderCreateDate = orderCreateDate;
    }

    public String getOrderFulfillmentDate() {
        return orderFulfillmentDate;
    }

    public void setOrderFulfillmentDate(String orderFulfillmentDate) {
        this.orderFulfillmentDate = orderFulfillmentDate;
    }

    public String getSkuId() {
        return skuId;
    }

    public void setSkuId(String skuId) {
        this.skuId = skuId;
    }

    public String getSellerId() {
        return sellerId;
    }

    public void setSellerId(String sellerId) {
        this.sellerId = sellerId;
    }

    public String getBuyerId() {
        return buyerId;
    }

    public void setBuyerId(String buyerId) {
        this.buyerId = buyerId;
    }

    public double getActualGmv() {
        return actualGmv;
    }

    public void setActualGmv(double actualGmv) {
        this.actualGmv = actualGmv;
    }

    public String getVenture() {
        return venture;
    }

    public void setVenture(String venture) {
        this.venture = venture;
    }

    public double getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(double unitPrice) {
        this.unitPrice = unitPrice;
    }

    public double getPaidPrice() {
        return paidPrice;
    }

    public void setPaidPrice(double paidPrice) {
        this.paidPrice = paidPrice;
    }
}
