package com.mapreduce2.OrderInversion;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairOfWords implements WritableComparable<PairOfWords> {

    private String leftElement;
    private String rightElement;

    public PairOfWords() {
    }

    public PairOfWords(String leftElement, String rightElement) {
        this.leftElement = leftElement;
        this.rightElement = rightElement;
    }

    public String getLeftElement() {
        return leftElement;
    }

    public void setLeftElement(String leftElement) {
        this.leftElement = leftElement;
    }

    public String getRightElement() {
        return rightElement;
    }

    public void setRightElement(String rightElement) {
        this.rightElement = rightElement;
    }

    public void setWord(String word) {
        setLeftElement(word);
    }

    @Override
    public int compareTo(PairOfWords pair) {
        String pl = pair.getLeftElement();
        String pr = pair.getRightElement();
        if (this.leftElement.equals(pl)) {
            return rightElement.compareTo(pr);
        }
        return this.leftElement.compareTo(pl);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.leftElement);
        Text.writeString(out, this.rightElement);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.leftElement = Text.readString(input);
        this.rightElement = Text.readString(input);
    }

    @Override
    public int hashCode() {
        return leftElement.hashCode() + rightElement.hashCode();
    }

    @Override
    public String toString() {
        return "(" + leftElement + ", " + rightElement + ")";
    }


    @Override
    public PairOfWords clone() {
        return new PairOfWords(this.leftElement, this.rightElement);
    }
}
