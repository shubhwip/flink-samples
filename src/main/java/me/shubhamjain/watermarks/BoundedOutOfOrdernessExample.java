package me.shubhamjain.watermarks;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class BoundedOutOfOrdernessExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a simple data stream with events
        DataStream<MyEvent> stream = env.fromElements(
                new MyEvent("A", 100, System.currentTimeMillis() - 5000),  // Out-of-order by 5 seconds
                new MyEvent("B", 200, System.currentTimeMillis()),         // In order
                new MyEvent("A", 150, System.currentTimeMillis() - 2000) ,  // Out-of-order by 2 seconds
                new MyEvent("C", 150, System.currentTimeMillis() - 20000),
                new MyEvent("D", 150, System.currentTimeMillis() + 20000)
        );

        // Assign timestamps and watermarks using BoundedOutOfOrdernessTimestampExtractor
        WatermarkStrategy<MyEvent> watermarkStrategy =
                WatermarkStrategy
                        .<MyEvent>forBoundedOutOfOrderness(
                                Duration.ofSeconds(3)
                        )
                        .withTimestampAssigner(
                                (myEvent, timestamp) ->
                                        myEvent.getTimestamp());
        // Print the events and watermarks
        stream
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .map(event -> event.toString())
                .print();

        env.execute("BoundedOutOfOrdernessExample");
    }

    // Simple event class
    public static class MyEvent {
        private String id;
        private int value;
        private long timestamp;

        public MyEvent(String id, int value, long timestamp) {
            this.id = id;
            this.value = value;
            this.timestamp = timestamp;
        }

        public String getId() {
            return id;
        }

        public int getValue() {
            return value;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "MyEvent{" +
                    "id='" + id + '\'' +
                    ", value=" + value +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}
