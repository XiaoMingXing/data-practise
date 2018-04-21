package com.mapreduce2.topN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class TopN_Driver {


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: TopN_Driver" + " <input-path> <output-path>");
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);



        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
