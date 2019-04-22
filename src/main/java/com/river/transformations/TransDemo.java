package com.river.transformations;

import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class TransDemo {
    public static void main(String[] args) throws Exception {
        List<VisitDto> list = Lists.newArrayList(
                new VisitDto(1,"river",30),
                new VisitDto(2,"frank",20),
                new VisitDto(2,"frank",20),
                new VisitDto(2,"frank",20)
        );
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(new VisitDto(1,"river",30),
                new VisitDto(2,"frank",20),
                new VisitDto(2,"frank",20),
                new VisitDto(2,"frank",20)).
                keyBy("id").print();
        env.execute();

    }
}
