FROM flink:1.17.0-scala_2.12
# Place the downloaded jar packages in the lib directory at the same level.
RUN usermod -aG sudo flink
COPY ./lib /opt/flink/lib
RUN rm -rf /tmp/hudi_table
RUN mkdir -p /tmp/hudi_table
RUN chmod -R 777 /tmp/hudi_table
RUN mkdir /opt/flink/plugins/flink-gs-fs-hadoop
COPY ./lib/flink-gs-fs-hadoop-1.17.0.jar /opt/flink/plugins/flink-gs-fs-hadoop/
RUN apt-get update && apt-get install tree
