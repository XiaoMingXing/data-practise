package com.realtime.stream.flink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.descriptors.Schema;

public class SchemaUtil {

    public static Schema getSalesOrderSchema() {
        return new Schema()
                .field("salesOrderId", "string")
                .field("salesOrderItemId", "string")
                .field("orderCreateDate", "string")
                .field("orderFulfillmentDate", "string")
                .field("skuId", "string")
                .field("sellerId", "string")
                .field("buyerId", "string")
                .field("actualGmv", "double")
                .field("venture", "string")
                .field("unitPrice", "string")
                .field("paidPrice", "double");
    }

    public static Schema getSalesOrderResultSchema() {
        return new Schema()
                .field("sellerId", "string")
                .field("revenue", "double")
                ;
    }
}
