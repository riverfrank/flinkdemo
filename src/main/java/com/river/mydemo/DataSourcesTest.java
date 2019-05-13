package com.river.mydemo;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Random;

/**
 * @author river
 * @date 2019/4/24 12:59
 **/
public class DataSourcesTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new MySource())
                .countWindowAll(10)
//                .evictor(DeltaEvictor.of(5, new DeltaFunction<Tuple2<String, Integer>>() {
//                    @Override
//                    public double getDelta(Tuple2<String, Integer> data0, Tuple2<String, Integer> data1) {
//                        System.out.println(data0 + "  " + data1);
//                        return data1.f1 - data0.f1;
//                    }
//                }))
                .evictor(CountEvictor.of(3))

                .min(1)
                .print();
        env.execute("DataSourcesTest");
    }

    public static class MySource implements SourceFunction<Tuple2<String,Integer>> {
        private static final long serialVersionUID = 1L;
        private volatile boolean isRunning = true;

        public MySource() {
            this.isRunning = true;
        }

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
            int i = 0;
            while (isRunning){
                Thread.sleep(1000);
                if(new Random().nextBoolean()){
                    sourceContext.collect(Tuple2.of("frank", i++));
                    continue;
                }
                sourceContext.collect(Tuple2.of("lucy", i++));
            }
        }
        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}