package com.river;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Collections;

/**
 * @author river
 * @date 2019/4/18 14:16
 **/
public class FlinkFirstDemo {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromElements("hello everyone","how are you")
                .flatMap((FlatMapFunction<String, String>) (value, out) -> {
                    for (String s : value.split(" ")) {
                        out.collect(s);
                    }
                })
                .returns(Types.STRING)
                .print();
        System.out.println("hello");
    }
}
