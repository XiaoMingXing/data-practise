package com.processing.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class WordSpout extends BaseRichSpout {

    // Q1: where we initilize the collector
    private SpoutOutputCollector collector;

    // todo we can practice to load the data from kafka
    private String[] words = {"hello", "world", "storm", "study"};

    private int index = 0;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        this.collector.emit(new Values(this.words[index]));
        index++;

        if (index >= words.length) {
            index = 0;
        }

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
