package com.mapreduce2.markov;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortComparator extends WritableComparator {

    public SecondarySortComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable first, WritableComparable second) {
        CompositeKey key1 = (CompositeKey) first;
        CompositeKey key2 = (CompositeKey) second;
        return key1.getCustomerId().compareTo(key2.getCustomerId());
    }
}
