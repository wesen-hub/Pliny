# dcp-agent 镜像: mc mirror 执行端(业务机), 平台任务下发 + 本地调度
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY settings-docker.xml /root/.m2/settings.xml
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -DskipTests dependency:go-offline || true
COPY src ./src
RUN --mount=type=cache,target=/root/.m2/repository mvn -B package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
ENV TZ=Asia/Shanghai
RUN curl -sfL -o /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-arm64/mc && chmod +x /usr/local/bin/mc
COPY --from=build /app/target/dcp-agent.jar app.jar
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-Xmx256m", "-jar", "app.jar"]
