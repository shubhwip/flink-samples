## Apache Flink With Java
## Flink and Java Compatibility
- Java 8(Deprecated) and Java 11.
- Experimental Support for Java 17.
https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/java_compatibility/

## Hudi Dependency Conflict with Iceberg
### remove hudi dependency as there is no version for 1.18
```shell
        <dependency>
            <groupId>org.apache.hudi</groupId>
            <artifactId>hudi-flink${flink.binary.version}-bundle</artifactId>
            <version>${hudi.version}</version>
        </dependency>
```