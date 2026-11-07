FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY src src
RUN mvn -B -ntp package -DskipTests
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/target/signalforge-1.0.0.jar /app/app.jar
COPY --from=build /build/target/lib /app/lib
RUN mkdir /data && chown 10001 /data
USER 10001
ENV SPARK_LOCAL_IP=127.0.0.1
ENTRYPOINT ["java","-Xmx1200m","--add-exports=java.base/sun.nio.ch=ALL-UNNAMED","--add-opens=java.base/java.nio=ALL-UNNAMED","--add-opens=java.base/java.lang.invoke=ALL-UNNAMED","-cp","/app/app.jar:/app/lib/*"]
CMD ["dev.ratnesh.signalforge.StreamingJob"]
