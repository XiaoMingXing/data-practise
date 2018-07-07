package com.mapreduce2.triangles;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CountTrianglesDriver {

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {

        Job job1 = Job.getInstance();
        job1.setJobName("triads");
        job1.setMapperClass(GraphEdgeMapper.class);
        job1.setReducerClass(GraphEdgeReducer.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(PairOfLongs.class);
        job1.setOutputValueClass(LongWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        // delete output folder to overwrite
        Path outputPath = new Path("triangles/tmp1");
        outputPath.getFileSystem(job1.getConfiguration()).delete(outputPath, true);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, outputPath);


        Job job2 = Job.getInstance();
        job2.setJobName("triangles");
        job2.setMapperClass(TriadsMapper.class);
        job2.setReducerClass(TriadsReducer.class);
        job2.setMapOutputKeyClass(PairOfLongs.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        Path outputPath2 = new Path("triangles/tmp2");
        outputPath.getFileSystem(job1.getConfiguration()).delete(outputPath2, true);

        FileInputFormat.addInputPath(job2, outputPath);
        FileOutputFormat.setOutputPath(job2, outputPath2);

        int status = job1.waitForCompletion(true) ? 0 : 1;
        if (status == 0) {
            status = job2.waitForCompletion(true) ? 0 : 1;
        }
        System.exit(status);
    }

}
