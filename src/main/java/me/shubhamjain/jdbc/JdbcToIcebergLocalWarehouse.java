package me.shubhamjain.jdbc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcToIcebergLocalWarehouse {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcToIcebergLocalWarehouse.class);
    private static final String GCP_PROJECT = System.getenv().getOrDefault("GCP_PROJECT", "");
    private static final String CHECKPOINTING_LOCATION = System.getenv()
            .getOrDefault("CHECKPOINTING_LOCATION", "");
    private static final long CHECKPOINTING_INTERVAL = Long.parseLong(
            System.getenv().getOrDefault("CHECKPOINTING_INTERVAL", "10000"));
    private static final String RESULT_LOCATION = System.getenv().getOrDefault("RESULT_LOCATION", "");
    private static final boolean LOCAL_EXECUTION = Boolean.parseBoolean(
            System.getenv().getOrDefault("LOCAL_EXECUTION", "true"));

    public static void main(String[] args) throws Exception {
        runFlinkJob(GCP_PROJECT, LOCAL_EXECUTION, CHECKPOINTING_LOCATION,
                CHECKPOINTING_INTERVAL, RESULT_LOCATION);
    }

    private static void runFlinkJob(
            String projectName, boolean local, String checkpointLocation,
            long checkpointInterval, String resultLocation) throws Exception {
        final StreamExecutionEnvironment env;
        final TypeInformation<?>[] fieldTypes =
                new TypeInformation<?>[] { BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DATE_TYPE_INFO, BasicTypeInfo.BIG_DEC_TYPE_INFO};

        final RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
        if (local) {
            env = LocalStreamEnvironment.createLocalEnvironment();
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance().inStreamingMode().build());

        String selectQuery = "select * from bookings";
        String driverName = "org.postgresql.Driver";
        String sourceDB = "demo";
        String sinkDB = "sink";
        String dbURL = "jdbc:postgresql://host.docker.internal:5433/";
        String dbPassword = "postgres";
        String dbUser = "postgres";

        //Define Input Format Builder
        tableEnvironment.executeSql("""
        CREATE TABLE bookings_source (
            `book_ref` STRING,
            `book_date` TIMESTAMP,
            `total_amount` DECIMAL(20, 0),
            PRIMARY KEY (`book_ref`) NOT ENFORCED
          ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://host.docker.internal:5433/demo',
            'username' = 'postgres',
            'password' = 'postgres',
            'table-name' = 'bookings'
          ); 
        """);
        // Get Data from SQL Table
        Table resultTable = tableEnvironment.sqlQuery("SELECT * FROM bookings_source");

        // Convert the result table to a DataStream and print it
        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnvironment.toRetractStream(resultTable, Row.class);
        //resultStream.print();

        tableEnvironment.executeSql("""
        CREATE CATALOG iceberg_catalog WITH (
            'type'='iceberg',
            'catalog-type'='hadoop',
            'warehouse'='file:///tmp/iceberg/warehouse/'
        );
        """);

        tableEnvironment.executeSql("""
        CREATE TABLE bookings_sink (
            `book_ref`    STRING,
            `book_date`     TIMESTAMP,
            `total_amount`  DECIMAL(20, 0),
            PRIMARY KEY (`book_ref`) ENFORCED
          ) WITH (
            'connector'='iceberg',
            'catalog-name'='iceberg_catalog',
            'catalog-type'='hadoop',
            'warehouse'='file:///tmp/iceberg/warehouse/',
            'format-version'='2'
          );
        """);

        tableEnvironment.executeSql("""
        INSERT INTO bookings_sink select * from bookings_source;
        """);
        // Start the Flink job execution
        env.execute("Flink Streaming JDBC to Table");
    }


}
