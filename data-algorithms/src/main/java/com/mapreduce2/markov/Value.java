package com.mapreduce2.markov;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Value implements WritableComparable<Value> {

    private String purchaseDate;
    private long amount;

    public Value() {
    }

    public Value(String purchaseDate, long amount) {
        this.purchaseDate = purchaseDate;
        this.amount = amount;
    }

    public String getPurchaseDate() {
        return purchaseDate;
    }

    public void setPurchaseDate(String purchaseDate) {
        this.purchaseDate = purchaseDate;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    @Override
    public int compareTo(Value value) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.purchaseDate);
        dataOutput.writeLong(this.amount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.purchaseDate = dataInput.readUTF();
        this.amount = dataInput.readLong();
    }
}
