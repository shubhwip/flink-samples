
Download Libs
```
export FLINK_VERSION=1.17
export FLINK_VERSION_FULL=1.17.0
export HUDI_VERSION=0.14.1
wget https://repo1.maven.org/maven2/org/apache/flink/flink-gs-fs-hadoop/${FLINK_VERSION_FULL}/flink-gs-fs-hadoop-${FLINK_VERSION_FULL}.jar -P ./lib/
wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-flink${FLINK_VERSION}-bundle/${HUDI_VERSION}/hudi-flink${FLINK_VERSION}-bundle-${HUDI_VERSION}.jar -P $FLINK_HOME/lib/
```


Start Flink SQL Client
```
docker-compose run sql-client
```

Create a database
```
psql -h host.docker.internal -p 5433 -U postgres
```
CREATE database demo;

Initiate a database
```
psql -h host.docker.internal -p 5433 -U postgres demo < ~/shubhamjain/de/alloy-vs-postgresql/demo-small-en-20170815.sql
```

Running flink program
If you see this error
```
Error: Unable to initialize main class me.shubhamjain.jdbc.JdbcToHudiGCS
Caused by: java.lang.NoClassDefFoundError: org/apache/flink/streaming/api/environment/StreamExecutionEnvironment
```

Then you have to choose `Add dependencies with "provided" scope to classpath`
