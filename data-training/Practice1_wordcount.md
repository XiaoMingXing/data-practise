##Data processing with MapReduce

### Practice 1 (WordCount)

#### Running in Intellij

- Add maven dependencies

```xml
 <dependencies>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>3.0.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>3.0.1</version>
        </dependency>
    </dependencies>
```

- Write Mapper to map the lines of words to <word,1> pairs

```java
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException {
        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
```

- Write Reducer to do the aggregation to reduce <word,1> , <word,1> to <word,2>

```java

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable count = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        count.set(sum);
        context.write(key, count);
    }
}

```

- Use the driver to orchestrate the mapper and reducer. and then make it work
```java

public class WordCountDriver {
    
    public static void main(String... args) throws IOException,
            ClassNotFoundException, InterruptedException {

        if (args.length < 2) {
            throw new IllegalArgumentException("Wrong arguments, at least two");
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        //input and output
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // this will improve the performance
        job.setCombinerClass(WordCountReducer.class);

        //delete the output dir
        outputDir.getFileSystem(conf).delete(outputDir);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

```
- generate input.txt file with below content
```text
Hello world
Hello mapReduce

```
- Run the driver with input.txt, we will get below output
```text
Hello	2
mapReduce	1
world	1
```

###Key Points
- The mapper output type of key and value should corresponded to the reducer input type of key and value
- The input type of key and value must be <LongWritable,Text>, 
because the default InputFormat is TextInputFormat, and it's key and value type are LongWritable and Text
- we can call fs.delete to delete the output folder everytime.
- we have to specify the output key,value type for Mapper and Reducer.


