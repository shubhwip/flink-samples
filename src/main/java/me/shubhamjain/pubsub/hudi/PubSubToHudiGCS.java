package me.shubhamjain.pubsub.hudi;

import me.shubhamjain.model.FlightRequestMetric;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;
import org.apache.hudi.client.clustering.plan.strategy.FlinkConsistentBucketClusteringPlanStrategy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

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

    // Create Hudi options for the data sink
    private static Map<String, String> createHudiOptions(String basePath) {
        Map<String, String> options = new HashMap<>();
        options.put(FlinkOptions.PATH.key(), basePath);
        options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
        options.put(FlinkOptions.PRECOMBINE_FIELD.key(), "ts");
        options.put(FlinkOptions.IGNORE_FAILED.key(), "true");
        options.put(FlinkOptions.WRITE_PARQUET_MAX_FILE_SIZE.key(), "-1");
        options.put(HoodieIndexConfig.BUCKET_INDEX_MIN_NUM_BUCKETS.key(), "1");
        options.put(HoodieIndexConfig.BUCKET_INDEX_MAX_NUM_BUCKETS.key(), "8");
        options.put(HoodieIndexConfig.BUCKET_SPLIT_THRESHOLD.key(), String.valueOf(1 / 1024.0 / 1024.0));
        options.put(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.key(), "1");
        options.put(FlinkOptions.INDEX_TYPE.key(), HoodieIndex.IndexType.BUCKET.name());
        options.put(FlinkOptions.OPERATION.key(), WriteOperationType.UPSERT.name());
        options.put(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED.key(), "true");
        options.put(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE.key(), HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING.name());
        options.put(FlinkOptions.CLUSTERING_PLAN_STRATEGY_CLASS.key(), FlinkConsistentBucketClusteringPlanStrategy.class.getName());
        options.put(HoodieClusteringConfig.EXECUTION_STRATEGY_CLASS_NAME.key(), "org.apache.hudi.client.clustering.run.strategy.SparkConsistentBucketClusteringExecutionStrategy");
        return options;
    }

    private static void runFlinkJob(
            String projectName, String subscriptionName, boolean local, String checkpointLocation, int checkpointInterval, String resultLocation) throws Exception {
        final StreamExecutionEnvironment env;
        if (local) {
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
