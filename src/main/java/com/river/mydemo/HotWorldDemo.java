package com.river.mydemo;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

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
              //  .timeWindow(Time.seconds(10), Time.seconds(5))

                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return new Tuple2<String, Integer>(value1.f0,value1.f1+value2.f1);
                    }
                })
                .timeWindowAll(Time.seconds(10), Time.seconds(5))
                .maxBy(1).print();
        env.execute("DataSourcesTest");
    }
}
