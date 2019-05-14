package com.river.mydemo;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.river.streaming.examples.wordcount.WordCountData;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author river
 * @date 2019/5/14 13:35
 **/
public class MySource implements SourceFunction<String> {
    private static final long serialVersionUID = 1L;
    private volatile boolean isRunning = true;

    public MySource() {
        this.isRunning = true;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        List<String> worlds = Arrays.stream(WordCountData.WORDS)
                .flatMap(t->  Splitter.on(CharMatcher.anyOf(",| !-?.")).omitEmptyStrings()
                        .trimResults()
                        .splitToList(t).stream()).collect(Collectors.toList());
        System.out.println("start");
        while (isRunning){
            Thread.sleep(500);
            String word = worlds.get(RandomUtils.nextInt(0,worlds.size()));
            sourceContext.collect(word);
            System.out.println("product new world :" +  word);
        }
    }
    @Override
    public void cancel() {
        isRunning = false;
    }
}

