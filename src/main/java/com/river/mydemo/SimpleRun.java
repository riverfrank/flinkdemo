package com.river.mydemo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author river
 * @date 2019/5/14 13:35
 **/
public class SimpleRun {
    /**
     *  测试添加了 数据源后，持续打印数据
     */
    public static void simpleRun() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new MySource())
                .print();
        env.execute("DataSourcesTest");
    }

    public static void main(String[] args) throws Exception {
        simpleRun();
    }
}
