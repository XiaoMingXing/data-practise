#### Use partitioner to control the distribution of the map output

```java
public class TopNPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        if (key.toString().startsWith("a")) {
            return 0;
        }
        return 1;
    }
}
```


####Set combiner to optimise performance

```java

job.setCombinerClass(TopNReducer.class);

```

#### Chain the wordcount and topN MR programe

```java


```