package me.shubhamjain.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class HudiReader {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Define the Hudi table (assuming it's already created and populated)
        String hudiTablePath = "file:///tmp/bucket-name/hudi/bookings/";

        tableEnv.executeSql(
                "CREATE TABLE hudi_table (" +
                        "  book_ref STRING," +
                        "  book_date TIMESTAMP(3)," +
                        "  total_amount DECIMAL(20, 0)," +
                        "  PRIMARY KEY (book_ref) NOT ENFORCED" +
                        ") PARTITIONED BY (book_ref) WITH (" +
                        "  'connector' = 'hudi'," +
                        "  'path' = '" + hudiTablePath + "'," +
                        "  'table.type' = 'COPY_ON_WRITE'," +
                        "  'hoodie.datasource.write.recordkey.field' = 'book_ref'," +
                        "  'hoodie.datasource.write.precombine.field' = 'book_date'," +
                        "  'write.precombine.field' = 'book_date'," +
                        "  'write.operation' = 'upsert'," +
                        "  'compaction.async.enabled' = 'false'," +
                        "  'hoodie.parquet.small.file.limit' = '104857600'" + // 100MB
                        ")"
        );

        // Query the Hudi table
        Table result = tableEnv.sqlQuery("SELECT * FROM hudi_table");

        // Convert the Table to a DataStream and print the results
        tableEnv.toDataStream(result).print();

        // Execute the Flink job
        env.execute("Read from Hudi Table");
    }
}
