package com.spark.secondarysort;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class TupleComparator implements Comparator<Tuple2<Integer, Integer>>, Serializable {
    @Override
    public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
        return o1._1.compareTo(o2._1);
    }
}
