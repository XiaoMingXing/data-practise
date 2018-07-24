##Data processing with MapReduce

### Practice 2 (TopN)

#### Running in Intellij

#####Writing the TopNMapper to read the data from the output of wordcount example
In this stage we get the top N in each of the mapper first to reduce the throughput 
```java

public class TopNMapper extends
        Mapper<Text, IntWritable, Text, IntWritable> {

    private final TreeMap<Integer, String> resultMap = new TreeMap<>();

    private int top_n;

    @Override
    public void setup(Context context) {
        this.top_n = context.getConfiguration().getInt("top.n", 5);
    }


    @Override
    public void map(Text key, IntWritable value, Context context) {
        int count = value.get();
        String word = key.toString();
        resultMap.put(count, word);
        if (resultMap.size() > this.top_n) {
            resultMap.pollFirstEntry();
        }
    }


    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (int count : resultMap.keySet()) {
            IntWritable value = new IntWritable(count);
            Text key = new Text(resultMap.get(count));
            context.write(key, value);
        }
    }
}
```
Some key point:
- We use Configuration object to set the top.n variable.
- We use TreeMap to sort the words and write the values to reduce function in cleanup
- In setup we initialise the topN number. in cleanup we commit the words into next phase
 
 
 #####Write the TopNReducer to get the final top N
 
 ```java
public class TopNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private TreeMap<Integer, String> resultMap = new TreeMap<>();
    private int topN = 5;

    @Override
    public void setup(Context context) {
        this.topN = context.getConfiguration().getInt("top.n", 5);
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int total = getTotalCount(values);
        resultMap.put(total, key.toString());
        if (resultMap.size() > topN) {
            resultMap.pollFirstEntry();
        }

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (int count : resultMap.keySet()) {
            IntWritable value = new IntWritable(count);
            Text key = new Text(resultMap.get(count));
            context.write(key, value);
        }
    }

    private int getTotalCount(Iterable<IntWritable> values) {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        return sum;
    }
}

```

Some tips:

-  We also use setup function to initialise top N value.
-  We also use TreeMap to sort the result and only keep top N inside of TreeMap
-  we write the top N into next phase in the cleanup funciton


##### Write TopNDriver to orchestrate different parts and running the MR program

```java
public class TopNDriver {

    public static void main(String... args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length < 2) {
            throw new IllegalArgumentException("Should have more than 2 arguments");
        }

        Configuration configuration = new Configuration();
        configuration.setInt("top.n", 3);

        Job job = Job.getInstance(configuration);
        Path inputPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inputPath);

        Path outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        job.setCombinerClass(TopNReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        outputDir.getFileSystem(configuration).delete(outputDir);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
```

Notes: in this case, we use KeyValueTextInputFormat to load the input. this class will load the lines as key-value pairs


#### Practice three - Customize partitioner



