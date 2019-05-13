package com.river.mydemo;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.river.streaming.example.wordcount.WordCountData;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * @author river
 * @date 2019/5/13 14:02
 * @desc  没 500 ms 产生一个随机数，并统计最后十秒钟 的 热num 排名
 **/
public class HotWorldDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new MySource())
                .map(t->new Tuple2<String,Integer>(t,1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .countWindowAll(10)
               // .evictor(CountEvictor.of(3))
                .reduce(new ReduceFunction<Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return new Tuple2<String, Integer>(value1.f0,value1.f1 + value2.f1);
                    }
                })
                .print();

        env.execute("DataSourcesTest");
    }

    /**
     *  测试添加了 数据源后，持续打印数据
     */
    public static void simpleRun() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new MySource())
                .print();
        env.execute("DataSourcesTest");
    }

    public static class MySource implements SourceFunction<String> {
        private static final long serialVersionUID = 1L;
        private volatile boolean isRunning = true;

        public MySource() {
            this.isRunning = true;
        }

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            List<String> worlds = Arrays.stream(WordCountData.WORDS)
                    .flatMap(t->  Splitter.on(CharMatcher.anyOf(",| !-")).omitEmptyStrings()
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
}
