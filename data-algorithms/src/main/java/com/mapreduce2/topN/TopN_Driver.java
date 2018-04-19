package com.mapreduce2.topN;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopN_Driver extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: TopN" + " <input-path> <output-path>");
        }

        int status = ToolRunner.run(new TopN_Driver(), args);
        System.exit(status);
    }


    @Override
    public int run(String[] args) throws Exception {
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Job job = Job.getInstance();
        job.setMapperClass(TopN_Mapper.class);

        return 0;
    }
}
