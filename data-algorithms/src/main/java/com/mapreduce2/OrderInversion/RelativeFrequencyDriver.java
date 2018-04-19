package com.mapreduce2.OrderInversion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RelativeFrequencyDriver {

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {

        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: RelativeFrequencyDriver" + " <input-path> <output-path>");
        }


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setJarByClass(RelativeFrequencyDriver.class);

        job.setOutputKeyClass(PairOfWords.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(RelativeFrequencyMapper.class);
        job.setReducerClass(RelativeFrequencyReducer.class);
        job.setPartitionerClass(OrderInversionPartitioner.class);
        job.setCombinerClass(RelativeFrequencyCombiner.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        // delete output folder to overwrite
        outputPath.getFileSystem(conf).delete(outputPath, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
