FROM gcr.io/distroless/java17:nonroot

COPY ./target/flink-samples-1.0-SNAPSHOT.jar /app.jar

ENTRYPOINT ["java", "--add-opens", "java.base/java.util=ALL-UNNAMED","--add-opens", "java.base/java.util.concurrent.atomic=ALL-UNNAMED", "--add-opens", "java.base/java.lang=ALL-UNNAMED", "-jar", "/app.jar"]