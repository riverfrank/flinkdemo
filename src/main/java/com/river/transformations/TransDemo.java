package com.river.transformations;

import com.sun.org.apache.regexp.internal.RE;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;

public class TransDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<VisitDto> visitDtoDataStream = env.fromElements(
                new VisitDto(1,"river",30),
                new VisitDto(2,"frank",20),
                new VisitDto(2,"frank",20),
                new VisitDto(2,"frank",20));
        visitDtoDataStream
                .map(t->  Tuple3.of(t.getId(),t.getName(),t.getAge()))
                .returns(Types.TUPLE(Types.INT,Types.STRING, Types.INT))
                .keyBy(0)
                .sum(0 ).print();

//        visitDtoDataStream.keyBy(0);

        env.execute();

    }
}
