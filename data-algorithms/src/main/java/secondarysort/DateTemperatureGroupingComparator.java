package secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


// use the comparator to group all the values into list. which only compare partial of key(year and month)

// Define the comparator that controls which keys are grouped together for a single call to
// Reducer.reduce(Object, Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
public class DateTemperatureGroupingComparator extends WritableComparator {

    public DateTemperatureGroupingComparator() {
        super(DateTemperaturePair.class, true);
    }

    public int compare(WritableComparable wc1, WritableComparable wc2) {
        DateTemperaturePair pair1 = (DateTemperaturePair) wc1;
        DateTemperaturePair pair2 = (DateTemperaturePair) wc2;
        return pair1.getYearMonth().compareTo(pair2.getYearMonth());
    }
}
