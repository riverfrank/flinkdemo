package com.river;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @author river
 * @date 2019/4/18 14:16
 **/
public class FlinkFirstDemo {
    public static void main(String[] args) throws IOException {
        Configuration config = new Configuration();
        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        System.out.print("Enter a Char:");
        char i = (char) System.in.read();
        System.out.println("your char is :"+i);
    }
}
