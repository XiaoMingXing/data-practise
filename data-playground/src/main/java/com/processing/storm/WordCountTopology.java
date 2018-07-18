package com.processing.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class WordCountTopology {

    private void runLocal(int waitSeconds) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("wordSpout", new WordSpout(), 1);
        builder.setBolt("countBolt", new CountBolt(), 1).shuffleGrouping("wordSpout");

        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word_count", config, builder.createTopology());

        // why we need to write this?
        try {
            Thread.sleep(waitSeconds * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        cluster.killTopology("word_count");
        cluster.shutdown();
    }

    public static void main(String... args) throws Exception {
        WordCountTopology wordCountTopology = new WordCountTopology();
        wordCountTopology.runLocal(60);

    }
}