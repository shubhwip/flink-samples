## Apache Flink With Java
## Flink and Java Compatibility
- Java 8(Deprecated) and Java 11.
- Experimental Support for Java 17.
https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/java_compatibility/

## Hudi Dependency Conflict with Iceberg
### Remove hudi dependency as there is no version for 1.18

- Error is
```shell
(8ff4c69f39ffc07bf436683211aa16f1_9dd63673dd41ea021b896d5203f3ba7c_1_0) switched from INITIALIZING to FAILED with failure cause:
java.lang.NoSuchMethodError: 'void org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper.<init>(com.codahale.metrics.Histogram)'
```
- Remove hudi dependency
```shell
        <dependency>
            <groupId>org.apache.hudi</groupId>
            <artifactId>hudi-flink${flink.binary.version}-bundle</artifactId>
            <version>${hudi.version}</version>
        </dependency>
```


## Resources
- Flink Examples - https://github.com/apache/flink/tree/master/flink-examples