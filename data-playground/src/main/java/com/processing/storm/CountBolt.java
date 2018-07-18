package com.processing.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CountBolt extends BaseRichBolt {

    private HashMap<String, Integer> wordMap = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {

        String word = tuple.getStringByField("word");

        int num = wordMap.getOrDefault(word, 0);
        wordMap.put(word, num + 1);

        Set<String> keys = wordMap.keySet();
        keys.forEach(key -> System.out.print(key + ":" + wordMap.get(key) + ";"));
        System.out.println();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
