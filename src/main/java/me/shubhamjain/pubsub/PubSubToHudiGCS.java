package me.shubhamjain.pubsub;

import me.shubhamjain.model.FlightRequestMetric;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class PubSubToHudiGCS {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubToHudiGCS.class);

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 3) {
            System.out.println(
                    "Missing parameters!\n"
                            + "Usage: flink run PubSub.jar --input-subscription <subscription> --input-topicName <topic> --output-topicName <output-topic> "
                            + "--google-project <google project name> ");
            return;
        }

        String projectName = parameterTool.getRequired("google-project");
        String inputTopicName = parameterTool.getRequired("input-topicName");
        String subscriptionName = parameterTool.getRequired("input-subscription");
        String checkpointLocation = parameterTool.getRequired("checkpoint-location");
        String resultLocation = parameterTool.getRequired("result-location");
        int checkpointInterval = Integer.parseInt(parameterTool.getRequired("checkpoint-interval"));
        boolean local = Boolean.parseBoolean(parameterTool.getRequired("local-environment"));
        runFlinkJob(projectName, subscriptionName, local, checkpointLocation, checkpointInterval, resultLocation);

    }

    private static void runFlinkJob(
            String projectName, String subscriptionName, boolean local, String checkpointLocation, int checkpointInterval, String resultLocation) throws Exception {
        final StreamExecutionEnvironment env;
        if(local) {
            env = StreamExecutionEnvironment.createLocalEnvironment();
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        // sets the checkpoint storage where checkpoint snapshots will be written
        env.getCheckpointConfig().setCheckpointStorage(checkpointLocation);

        env.enableCheckpointing(checkpointInterval);

        PubSubSource<FlightRequestMetric> pubSubSource = PubSubSource.newBuilder()
                .withDeserializationSchema(new FlightRequestDeserialiser())
                .withProjectName(projectName)
                .withSubscriptionName(subscriptionName)
                .withPubSubSubscriberFactory(100, Duration.ofSeconds(100), 5)
                .withMessageRateLimit(1)
                .build();

        // 3. Create a watermark strategy
        WatermarkStrategy<FlightRequestMetric> watermarkStrategy =
                WatermarkStrategy
                        .<FlightRequestMetric>forBoundedOutOfOrderness(
                                Duration.ofSeconds(20)
                        )
                        .withTimestampAssigner(
                                (flightRequestMetric, created_at) -> Long.parseLong(flightRequestMetric.getCreatedAt().toString())
                        );

        // 4. Create the stream
        DataStream<FlightRequestMetric> txnStream =
                env
                        .addSource(
                                pubSubSource,
                                "Flight Data Source"
                        )
                        .setParallelism(5)
                        .name("FlightSource")
                        .uid("FlightSource");

        txnStream.print();

        env.execute("Flink Streaming PubSubReader");
    }
}
