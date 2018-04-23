package com.mapreduce2.movingAverage;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


// use the comparator to group all the values into list. which only compare partial of key(year and month)

// Define the comparator that controls which keys are grouped together for a single call to
// Reducer.reduce(Object, Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
public class CompositeKeyComparator extends WritableComparator {

    public CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }

    public int compare(WritableComparable wc1, WritableComparable wc2) {
        CompositeKey pair1 = (CompositeKey) wc1;
        CompositeKey pair2 = (CompositeKey) wc2;
        return pair1.compareTo(pair2);
    }
}
