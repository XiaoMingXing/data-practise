package com.mapreduce2.markov;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable<CompositeKey> {

    private String customerId;
    private String purchaseDate;

    public CompositeKey() {
    }

    public CompositeKey(String customerId, String purchaseDate) {
        this.customerId = customerId;
        this.purchaseDate = purchaseDate;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getPurchaseDate() {
        return purchaseDate;
    }

    public void setPurchaseDate(String purchaseDate) {
        this.purchaseDate = purchaseDate;
    }

    @Override
    public int compareTo(CompositeKey compositeKey) {
        if (this.customerId.compareTo(compositeKey.customerId) == 0) {
            return this.purchaseDate.compareTo(compositeKey.purchaseDate);
        } else {
            return this.customerId.compareTo(compositeKey.customerId);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.customerId);
        dataOutput.writeUTF(this.purchaseDate);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.customerId = dataInput.readUTF();
        this.purchaseDate = dataInput.readUTF();
    }
}
