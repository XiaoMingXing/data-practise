package com.mapreduce2.OrderInversion;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static jersey.repackaged.com.google.common.collect.ImmutableMap.of;

public class RelativeFrequencyMapper extends Mapper<LongWritable, Text, PairOfWords, IntWritable> {

    private int neighborWindow = 2;

    public void setup(Context context) {
        this.neighborWindow = context.getConfiguration().getInt("neighbor.window", 2);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        List<Map<PairOfWords, IntWritable>> pairOfWords = splitToPairOfWords(value.toString());
        if (CollectionUtils.isEmpty(pairOfWords)) {
            return;
        }
        pairOfWords.forEach((pairOfWordMap) -> {
            pairOfWordMap.forEach((pair, count) -> {
                try {
                    context.write(pair, count);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });
    }

    List<Map<PairOfWords, IntWritable>> splitToPairOfWords(String line) {
        List<Map<PairOfWords, IntWritable>> pairOfWords = newArrayList();
        String[] tokens = StringUtils.split(line, ' ');
        if (tokens.length < 2) {
            return null;
        }
        for (int index = 0; index < tokens.length; index++) {
            PairOfWords pair = new PairOfWords();
            String word = tokens[index];
            word.replaceAll("\\W+", "");
            pair.setLeftElement(word);
            int start = index > neighborWindow ? index - neighborWindow : 0;
            int end = index >= tokens.length - neighborWindow ? tokens.length - 1 : index + neighborWindow;
            for (int cursor = start; cursor <= end; cursor++) {
                // skip current word
                if (cursor == index) {
                    continue;
                }
                pair.setRightElement(tokens[cursor]);
                // can replace with context.write
                pairOfWords.add(of(pair.clone(), new IntWritable(1)));
            }
            pair.setRightElement("*");
            // can replace with context.write
            pairOfWords.add(of(pair.clone(), new IntWritable(end - start)));
        }
        return pairOfWords;

    }
}
