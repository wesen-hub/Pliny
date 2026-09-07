# 湖同步作业(dcp-lakesync)构建(多阶段; 构建期跑无 Spark 核心自检)
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY settings-docker.xml /root/.m2/settings.xml
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2/repository mvn -B -DskipTests dependency:go-offline || true
COPY src ./src
RUN --mount=type=cache,target=/root/.m2/repository mvn -B package -DskipTests
# 核心自检(无 Spark): 配置解析/谓词构建 —— 证明 shade jar 可用
RUN java -cp /app/target/dcp-lakesync.jar com.dcp.lakesync.LakeSyncSmokeMain

FROM eclipse-temurin:17-jre
WORKDIR /app
ENV TZ=Asia/Shanghai
COPY --from=build /app/target/dcp-lakesync.jar app.jar
# 运行镜像占位: 作业实际以 spark-submit 形态跑在 dev/集群(与 tagengine-executor 同模式)
ENTRYPOINT ["java", "-cp", "/app/app.jar", "com.dcp.lakesync.LakeSyncSmokeMain"]
