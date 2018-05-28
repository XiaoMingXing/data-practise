package com.mapreduce2.markov;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Application {


    public static void main(String[] args)
            throws IOException,
            ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            throw new IllegalArgumentException("Secondary sort MapReduce Usage: " + " <input-path> <output-path>");
        }

        String inputPath = args[0];
        String outputPath = args[1];

        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(Value.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        Path outputDir = new Path(outputPath);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setMapperClass(SecondarySortMapper.class);
        job.setGroupingComparatorClass(SecondarySortComparator.class);

        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setReducerClass(SecondarySortReducer.class);

        outputDir.getFileSystem(config).delete(outputDir, true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
