package me.shubhamjain.setup;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamExecutionEnvironmentSetUp {
    public static void main(String[] args) {
        // For running programs by submitting jobs to flink cluster
        StreamExecutionEnvironment remoteEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // For running programs locally from your IDE without submitting jobs to flink cluster
        StreamExecutionEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment();

    }
}
